package com._42six.amino.bitmap;

import com._42six.amino.common.*;
import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.datacache.BucketCache;
import com._42six.amino.common.util.PathUtils;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

public class BitLookupJob extends Configured implements Tool {
    private static final String BIGTABLE_INSTANCE = "bigtable.instance";
    private static final String BIGTABLE_ZOOKEEPERS = "bigtable.zookeepers";
    private static final String BIGTABLE_USERNAME = "bigtable.username";
    private static final String BIGTABLE_PASSWORD = "bigtable.password";

    private static final String AMINO_NUM_REDUCERS = "amino.num.reducers";
    private static final String AMINO_NUM_REDUCERS_BITMAP = "amino.num.reducers.job.bitmap";
    private static final int DEFAULT_NUM_REDUCERS = 14;

    private static final String AMINO_NUM_TABLETS = "amino.bigtable.number.of.shards";
    private static final String AMINO_NUM_TABLETS_HYPOTHESIS = "amino.bigtable.number.of.shards.hypothesis";

    private static boolean recreateTables(Configuration conf) throws IOException {
        //AminoConfiguration.loadDefault(conf, "AminoDefaults", true);
        String instanceName = conf.get(BIGTABLE_INSTANCE);
        String zooKeepers = conf.get(BIGTABLE_ZOOKEEPERS);
        String user = conf.get(BIGTABLE_USERNAME);
        String password = conf.get(BIGTABLE_PASSWORD);
        String indexTable = conf.get("amino.bitmap.indexTable");
        boolean blastIndex = conf.getBoolean("amino.bitmap.first.run", true); //should always assume it's the first run unless specified

        Instance inst = new ZooKeeperInstance(instanceName, zooKeepers);
        TableOperations tableOps;
        try {
//            tableOps = inst.getConnector(user, new PasswordToken(password)).tableOperations();
            tableOps = inst.getConnector(user, password).tableOperations();
        } catch (AccumuloException ex) {
            throw new IOException(ex);
        } catch (AccumuloSecurityException ex) {
            throw new IOException(ex);
        }

        return IteratorUtils.createTable(tableOps, indexTable, blastIndex, blastIndex);
    }

    public int run(String[] args) throws Exception {

        System.out.println("\n================================ BitLookup Job ================================\n");
        Configuration conf = getConf();

        if(!recreateTables(conf)){
            return 1;
        }

        final String inputDir = args[0]; // TODO: use configuration instead of positional argument

        final Job job = new Job(conf, "Amino feature index job");
        job.setJarByClass(BitLookupJob.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        final String inputPaths = StringUtils.join(PathUtils.getJobDataPaths(conf, inputDir), ','); // TODO: use configuration instead of positional argument
        System.out.println("Input paths: [" + inputPaths + "].");

        final String cachePaths = StringUtils.join(PathUtils.getJobCachePaths(conf, inputDir), ',');
        System.out.println("Cache paths: [" + cachePaths + "].");

        PathUtils.setCachePath(job.getConfiguration(), cachePaths);
        SequenceFileInputFormat.setInputPaths(job, inputPaths);
        job.setMapperClass(BitLookupMapper.class);
        job.setMapOutputKeyClass(BitLookupKey.class);
        job.setMapOutputValueClass(BitmapValue.class);
        job.setCombinerClass(BitLookupCombiner.class); // TODO Add this back in
        job.setReducerClass(BitLookupReducer.class);
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Value.class);

        final String workingDirectory = args[2]; // TODO: use configuration instead of positional argument
        JobUtilities.deleteDirectory(this.getConf(), workingDirectory);

        int numTablets = -1;
        if (args.length > 3){
            numTablets = Integer.parseInt(args[3]);
        }

        return execute(job, inputDir, workingDirectory, numTablets);
    }


    public int execute(Job job, String inputDir, String workingDir, int numTabletsCommandLine) throws IOException, InterruptedException, ClassNotFoundException
    {
        final Configuration conf = job.getConfiguration();
        final String instanceName = conf.get(BIGTABLE_INSTANCE);
        final String zooKeepers = conf.get(BIGTABLE_ZOOKEEPERS);
        final String user = conf.get(BIGTABLE_USERNAME);
        final String password = conf.get(BIGTABLE_PASSWORD);
        final String tableName = conf.get("amino.bitmap.indexTable");
        final String temp = IteratorUtils.TEMP_SUFFIX;
        final boolean blastIndex = conf.getBoolean("amino.bitmap.first.run", true); //should always assume it's the first run unless specified

        final String splitfile = workingDir + "/featureSplits.txt";

        Connector c = null;

        boolean success = false;
        try
        {
            final Instance inst = new ZooKeeperInstance(instanceName, zooKeepers);
            c = inst.getConnector(user, password);

            //set number of reducers
            int numReducers = conf.getInt(AMINO_NUM_REDUCERS_BITMAP, 0);
            if (numReducers > 0) {
                job.setNumReduceTasks(numReducers);
            }
            else {
                job.setNumReduceTasks(conf.getInt(AMINO_NUM_REDUCERS, DEFAULT_NUM_REDUCERS));
            }

            int numTablets = conf.getInt(AMINO_NUM_TABLETS, -1);
            int numTabletsHypothesis = conf.getInt(AMINO_NUM_TABLETS_HYPOTHESIS, -1);

            if (numTabletsCommandLine != -1) {
                numReducers = numTabletsCommandLine;
                System.out.println("Using number of reducers/tablets specified at command line ["
                        + numReducers + "]");
            }
            else if (numTabletsHypothesis != -1) {
                numReducers = numTabletsHypothesis;
                System.out.println("Using number of reducers/tablets specified in the config property ["
                        + AMINO_NUM_TABLETS_HYPOTHESIS + "] - [" + numReducers + "]");
            }
            else if (numTablets != -1) {
                numReducers = numTablets;
                System.out.println("Using number of reducers/tablets specified in the config property ["
                        + AMINO_NUM_TABLETS + "] - [" + numReducers + "]");
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

            success = IteratorUtils.createTable(c.tableOperations(), tableName, splits, blastIndex, blastIndex);

            job.setOutputFormatClass(AccumuloFileOutputFormat.class);
            AccumuloFileOutputFormat.setOutputPath(job, new Path(workingDir + "/files"));
            // job.setPartitionerClass(RangePartitioner.class);
            // job.setSortComparatorClass(FeatureKeyComparator.class); // This will ensure the values come in sorted so we don't have to do that TreeMap...
            // RangePartitioner.setSplitFile(job, splitfile);
        }
        catch (AccumuloException e)
        {
            e.printStackTrace();
        }
        catch (AccumuloSecurityException e)
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
                String tb = tableName + temp;
                if (!blastIndex){
                    tb = tableName;
                }
                System.out.println("Importing the files in '" + workingDir + "/files' to the table: " + tb);
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
        final int numberOfHashes = conf.getInt("amino.bitmap.num-hashes", 1);
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
        final Configuration conf = new Configuration();
        conf.set(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY, args[1]); // TODO: use flag instead of positional
        AminoConfiguration.loadDefault(conf, "AminoDefaults", true);

        final int res = ToolRunner.run(conf, new BitLookupJob(), args);
        System.exit(res);
    }
}
