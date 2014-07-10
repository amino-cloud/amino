package com._42six.amino.bitmap;

import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.ByBucketKey;
import com._42six.amino.common.JobUtilities;
import com._42six.amino.common.accumulo.IteratorUtils;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Parses the data and populates the byBucket table
 */
public class ByBucketJob extends BitmapJob {

    public int run(String[] args) throws Exception {
        System.out.println("\n================================ ByBucket Job ================================\n");

        // Create the command line options to be parsed
        final Option o1 = new Option("o", "outputDir", true, "The output directory");
        final Option o2 = new Option("w", "workingDir", true, "The working directory");
        final Option o3 = new Option("t", "numTablets", false, "The number of tablets in use");

        initializeConfigAndOptions(args, Optional.of(Sets.newHashSet(o1, o2, o3)));
        final Configuration conf = getConf();
        loadConfigValues(conf);

        if(!recreateTable(conf.get(AminoConfiguration.TABLE_BUCKET), conf.getInt(AminoConfiguration.NUM_SHARDS, 10))){
            return 1;
        }

        final Job job = new Job(conf, "Amino bucket index job");
        job.setJarByClass(ByBucketJob.class);
        initializeJob(job);

        job.setMapperClass(ByBucketMapper.class);
        job.setMapOutputKeyClass(ByBucketKey.class);
        job.setMapOutputValueClass(BitmapValue.class);
        job.setCombinerClass(ByBucketCombiner.class);
        job.setReducerClass(ByBucketReducer.class);
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Value.class);

        final int numTablets = Integer.parseInt(fromOptionOrConfig(Optional.of("t"), Optional.<String>absent(), "-1"));
        final String workingDirectory = fromOptionOrConfig(Optional.of("w"), Optional.of(AminoConfiguration.WORKING_DIR));
        JobUtilities.resetWorkingDirectory(this.getConf(), workingDirectory);

        return execute(job, workingDirectory, numTablets);
    }

    public int execute(Job job, String workingDir, int numTabletsCommandLine) throws IOException, InterruptedException, ClassNotFoundException
    {
        final Configuration conf = job.getConfiguration();
        final String tableName = conf.get(AminoConfiguration.TABLE_BUCKET);
        final String splitFile = workingDir + "/bucketSplits.txt";

        Connector c = null;

        boolean success = false;
        try
        {
            final Instance inst = new ZooKeeperInstance(instanceName, zooKeepers);
            c = inst.getConnector(user, new PasswordToken(password));

            // set number of reducers TODO - Clean this up
            int numReducers = conf.getInt(AminoConfiguration.NUM_REDUCERS, 0);
            if (numReducers > 0) {
                job.setNumReduceTasks(numReducers);
            }
            else {
                numReducers = conf.getInt(AminoConfiguration.NUM_REDUCERS, AminoConfiguration.DEFAULT_NUM_REDUCERS);
                job.setNumReduceTasks(numReducers);
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
                System.out.println("Number of bucket reducers/tablets not specified in config or "
                        + "last argument. Using the number of reducers instead [" + numReducers + "]");
            }

            System.out.println("Setting number of reducers: " + numReducers);

            final SortedSet<Text> splits = new TreeSet<Text>();
            job.setNumReduceTasks(numReducers);

            final FileSystem fs = FileSystem.get(conf);
            final PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(new Path(splitFile))));
            for (int i = 0; i < numReducers; i++)
            {
                Text split = new Text(Integer.toString(i));
                out.println(new String(Base64.encodeBase64(TextUtil.getBytes(split))));
            }
            out.flush();
            out.close();

            success = IteratorUtils.createTable(c.tableOperations(), tableName, tableContext, splits, blastIndex, blastIndex);

            job.setOutputFormatClass(AccumuloFileOutputFormat.class);
            AccumuloFileOutputFormat.setOutputPath(job, new Path(workingDir + "/files"));

            //job.setPartitionerClass(ByBucketPartitioner.class); // TODO Fix bug in Partitioner
            //ByBucketPartitioner.setSplitFile(job, splitFile);
            //job.setSortComparatorClass(BucketKeyComparator.class); // This will ensure the values come in sorted so we don't have to do that TreeMap...
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
            System.out.println("ByBucketJob MapReduce job failed. Job results will not be imported into Accumulo.");
        }
        else if (c != null && success) {
            System.out.println("Importing job results to Accumulo....");
            try
            {
                final String tb = (!blastIndex) ? tableName : tableName + AminoConfiguration.TEMP_SUFFIX;
                final String filesPath = workingDir + "/files";
                final String failuresPath = workingDir + "/failures";
                System.out.println("Importing the files in '" + filesPath + "' to the table: " + tb);
                JobUtilities.setGroupAndPermissions(conf, workingDir);
                c.tableOperations().importDirectory(tb, filesPath, failuresPath, false);
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

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ByBucketJob(), args));
    }

}
