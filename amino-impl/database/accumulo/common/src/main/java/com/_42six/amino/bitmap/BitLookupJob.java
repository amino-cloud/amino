package com._42six.amino.bitmap;

import com._42six.amino.common.*;
import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.datacache.BucketCache;
import com._42six.amino.common.util.PathUtils;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.cli.Option;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

public class BitLookupJob extends BitmapJob {
    public int run(String[] args) throws Exception {
        System.out.println("\n================================ BitLookup Job ================================\n");

        // Create the command line options to be parsed
        final Option o1 = new Option("o", "outputDir", true, "The output directory");
        final Option o2 = new Option("w", "workingDir", true, "The working directory");
        final Option o3 = new Option("t", "numTablets", false, "The number of tablets in use");

        initializeConfigAndOptions(args, Optional.of(Sets.newHashSet(o1, o2, o3)));
        final Configuration conf = getConf();
        loadConfigValues(conf);

        if(!recreateTable(conf.get(AminoConfiguration.TABLE_INDEX))){
            return 1;
        }

        final String inputDir = fromOptionOrConfig(Optional.of("o"), Optional.of(AminoConfiguration.OUTPUT_DIR));
        PathUtils.pathsExists(inputDir, conf);

        final Job job = new Job(conf, "Amino feature index job");
        job.setJarByClass(BitLookupJob.class);
        initializeJob(job);

        job.setMapperClass(BitLookupMapper.class);
        job.setMapOutputKeyClass(BitLookupKey.class);
        job.setMapOutputValueClass(BitmapValue.class);
        job.setCombinerClass(BitLookupCombiner.class);
        job.setReducerClass(BitLookupReducer.class);
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Value.class);

        int numTablets = Integer.parseInt(fromOptionOrConfig(Optional.of("t"), Optional.<String>absent(), "-1"));
        final String workingDirectory = fromOptionOrConfig(Optional.of("w"), Optional.of(AminoConfiguration.WORKING_DIR));
        JobUtilities.resetWorkingDirectory(this.getConf(), workingDirectory);

        return execute(job, inputDir, workingDirectory, numTablets);
    }


    public int execute(Job job, String inputDir, String workingDir, int numTabletsCommandLine) throws IOException, InterruptedException, ClassNotFoundException
    {
        final Configuration conf = job.getConfiguration();
        final String tableName = conf.get(AminoConfiguration.TABLE_INDEX);
        final boolean blastIndex = conf.getBoolean(AminoConfiguration.FIRST_RUN, true); //should always assume it's the first run unless specified

        final String splitfile = workingDir + "/featureSplits.txt";

        Connector c = null;

        boolean success = false;
        try
        {
            final Instance inst = new ZooKeeperInstance(instanceName, zooKeepers);
            c = inst.getConnector(user, new PasswordToken(password));

            //set number of reducers
            int numReducers = conf.getInt(AminoConfiguration.NUM_REDUCERS, 0);
            if (numReducers > 0) {
                job.setNumReduceTasks(numReducers);
            }
            else {
                job.setNumReduceTasks(conf.getInt(AminoConfiguration.NUM_REDUCERS, AminoConfiguration.DEFAULT_NUM_REDUCERS));
            }

            int numTablets = conf.getInt(AminoConfiguration.NUM_SHARDS, -1);
            int numTabletsHypothesis = conf.getInt(AminoConfiguration.NUM_SHARDS_HYPOTHESIS, -1);

            if (numTabletsCommandLine != -1) {
                numReducers = numTabletsCommandLine;
                System.out.println("Using number of reducers/tablets specified at command line ["
                        + numReducers + "]");
            }
            else if (numTabletsHypothesis != -1) {
                numReducers = numTabletsHypothesis;
                System.out.println("Using number of reducers/tablets specified in the config property ["
                        + AminoConfiguration.NUM_SHARDS_HYPOTHESIS + "] - [" + numReducers + "]");
            }
            else if (numTablets != -1) {
                numReducers = numTablets;
                System.out.println("Using number of reducers/tablets specified in the config property ["
                        + AminoConfiguration.NUM_SHARDS + "] - [" + numReducers + "]");
            }
            else {
                System.out.println("Number of hypothesis reducers/tablets not specified in config or "
                        + "last argument. Using the number of reducers instead [" + numReducers + "]");
            }

            final SortedSet<Text> splits = buildSplitsFromSample(conf, inputDir, numReducers);
            job.setNumReduceTasks(splits.size() + 1);

            final FileSystem fs = FileSystem.get(conf);
            final PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(new Path(splitfile))));
            for (Text split : splits)
            {
                out.println(new String(Base64.encodeBase64(TextUtil.getBytes(split))));
            }
            out.flush();
            out.close();

            success = IteratorUtils.createTable(c.tableOperations(), tableName, tableContext, splits, blastIndex, blastIndex);

            job.setOutputFormatClass(AccumuloFileOutputFormat.class);
            AccumuloFileOutputFormat.setOutputPath(job, new Path(workingDir + "/files"));
            // job.setPartitionerClass(RangePartitioner.class);
            // job.setSortComparatorClass(FeatureKeyComparator.class); // This will ensure the values come in sorted so we don't have to do that TreeMap...
            // RangePartitioner.setSplitFile(job, splitfile);
        }
        catch (AccumuloException | AccumuloSecurityException e)
        {
            e.printStackTrace();
        }

        int result = 0;
        if (success)
        {
            result = job.waitForCompletion(true) ? 0 : -1;
        }

        if (result != 0) {
            System.out.println("BitLookupJob MapReduce job failed. Job results will not be imported into Accumulo.");
        }
        else if (c != null && success) {
            System.out.println("Importing job results to Accumulo....");
            try
            {
                final String tb = (!blastIndex) ? tableName : tableName + AminoConfiguration.TEMP_SUFFIX;
                System.out.println("Importing the files in '" + workingDir + "/files' to the table: " + tb);
                JobUtilities.setGroupAndPermissions(conf, workingDir);
                c.tableOperations().importDirectory(tb, workingDir + "/files", workingDir + "/failures", false);
                result = JobUtilities.failureDirHasFiles(conf, workingDir + "/failures");
            }
            catch (Exception e)
            {
                result = 1;
                e.printStackTrace();
            }
        }
        else {
            System.out.println("Not importing job results into Accumulo because of a previous error.");
        }

        return result;
    }

    // This is an attempt to avoid hot spots when running the hypothesis job, there really is no way to guarantee this
    // unless we run through all the files
    private SortedSet<Text> buildSplitsFromSample(Configuration conf, String inputDir, int numReducers) throws IOException
    {
        final int numberOfHashes = conf.getInt(AminoConfiguration.NUM_HASHES, 1);
        final FileSystem fs = FileSystem.get(conf);

        final ArrayList<FileStatus> vettedStatus = IngestUtilities.grabAllVettedFileStati(conf, fs, inputDir);

        // pick a random file
        final int randomNum =  (new Random()).nextInt(vettedStatus.size());
        FileStatus status = vettedStatus.get(randomNum);
        System.out.println("Reading random file: " + status.getPath().toString() + " to determine appropriate splits...");

        // Grab all the indexes in this sequence file
        final List<Integer> indexes = new ArrayList<Integer>();
        final SequenceFile.Reader reader = new SequenceFile.Reader(fs, status.getPath(), conf);
        final BucketCache bucketCache = new BucketCache(conf);
        final Writable key = new BucketStripped();
        final Writable val = new AminoWritable();
        while(reader.next(key, val))
        {
            final Feature feature = ((AminoWritable)val).getFeature();
            final FeatureFact featureFact = ((AminoWritable)val).getFeatureFact();
            final Bucket bucket = bucketCache.getBucket((BucketStripped)key);
            for (int i = 0; i < numberOfHashes; i++)
            {
                final int featureFactIndex = BitmapIndex.getFeatureFactIndex(bucket, feature, featureFact, i);
                indexes.add(featureFactIndex);
            }
        }
        System.out.println("Found " + indexes.size() + " indexes in " + status.getPath().toString());
        IOUtils.closeStream(reader);

        //Build the percentiles based on the indexes and amount of reducers
        final SortedSet<Text> splits = new TreeSet<Text>();
        final double[] vals = new double[indexes.size()];
        for (int i = 0; i < indexes.size(); i++)
        {
            vals[i] = indexes.get(i);
        }
        final Percentile p = new Percentile();
        p.setData(vals);
        final double cutWidth = 100.0 / (double)numReducers;
        double window = cutWidth;
        while (window <= (100.0 - cutWidth))
        {
            final double split = p.evaluate(window);
            splits.add(new Text(Integer.toString((int)split)));
            window += cutWidth;
        }

        return splits;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new BitLookupJob(), args));
    }
}
