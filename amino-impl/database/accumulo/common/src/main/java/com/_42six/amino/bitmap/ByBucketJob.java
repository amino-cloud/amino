package com._42six.amino.bitmap;

import com._42six.amino.common.ByBucketKey;
import com._42six.amino.common.JobUtilities;
import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.bigtable.TableConstants;
import com._42six.amino.common.util.PathUtils;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.cli.Option;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.SortedSet;
import java.util.TreeSet;

public class ByBucketJob extends BitmapJob {

    private static final String AMINO_NUM_REDUCERS = "amino.num.reducers";
    private static final String AMINO_NUM_REDUCERS_BITMAP = "amino.num.reducers.job.bitmap";
    private static final int DEFAULT_NUM_REDUCERS = 14;

    private static final String AMINO_NUM_TABLETS = "amino.bigtable.number.of.shards";
    private static final String AMINO_NUM_TABLETS_HYPOTHESIS = "amino.bigtable.number.of.shards.hypothesis";

    private static boolean recreateTables(Configuration conf) throws IOException {
        //AminoConfiguration.loadDefault(conf, "AminoDefaults", true);
        String instanceName = conf.get(TableConstants.CFG_INSTANCE);
        String zooKeepers = conf.get(TableConstants.CFG_ZOOKEEPERS);
        String user = conf.get(TableConstants.CFG_USER);
        String password = conf.get(TableConstants.CFG_PASSWORD);
        String bucketTable = conf.get("amino.bitmap.bucketTable");
        final String tableContext = conf.get("amino.tableContext", "amino");
        boolean blastIndex = conf.getBoolean("amino.bitmap.first.run", true); //should always assume it's the first run unless specified

        Instance inst = new ZooKeeperInstance(instanceName, zooKeepers);
        TableOperations tableOps;
        try {
            tableOps = inst.getConnector(user, password).tableOperations();
        } catch (AccumuloException ex) {
            throw new IOException(ex);
        } catch (AccumuloSecurityException ex) {
            throw new IOException(ex);
        }

        int numShards = conf.getInt(BitmapConfigHelper.BITMAP_CONFIG_NUM_SHARDS, 10);
        return  IteratorUtils.createTable(tableOps, bucketTable, tableContext, numShards, blastIndex, blastIndex);
    }

    public int run(String[] args) throws Exception {
        System.out.println("\n================================ ByBucket Job ================================\n");

        // Create the command line options to be parsed
        final Option o1 = new Option("o", "outputDir", true, "The output directory");
        final Option o2 = new Option("w", "workingDir", true, "The working directory");
        final Option o3 = new Option("t", "numTablets", false, "The number of tablets in use");
//        o1.setRequired(true);
//        o2.setRequired(true);

        initializeConfigAndOptions(args, Optional.of(Sets.newHashSet(o1, o2, o3)));
        final Configuration conf = getConf();

        if(!recreateTables(conf)){
            return 1;
        }

        final String inputDir = fromOptionOrConfig(Optional.of("o"), Optional.of(CONF_OUTPUT_DIR));

        final Job job = new Job(conf, "Amino bucket index job");
        job.setJarByClass(ByBucketJob.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        final String inputPaths = StringUtils.join(PathUtils.getJobDataPaths(conf, inputDir), ',');
        System.out.println("Input paths: [" + inputPaths + "].");

        final String cachePaths = StringUtils.join(PathUtils.getJobCachePaths(conf, inputDir), ',');
        System.out.println("Cache paths: [" + cachePaths + "].");

        PathUtils.setCachePath(job.getConfiguration(), cachePaths);
        SequenceFileInputFormat.setInputPaths(job, inputPaths);
        job.setMapperClass(ByBucketMapper.class);
//        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputKeyClass(ByBucketKey.class);
        job.setMapOutputValueClass(BitmapValue.class);
        job.setCombinerClass(ByBucketCombiner.class);
        job.setReducerClass(ByBucketReducer.class);
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Value.class);

        int numTablets = Integer.parseInt(fromOptionOrConfig(Optional.of("t"), Optional.<String>absent(), "-1"));
        final String workingDirectory = fromOptionOrConfig(Optional.of("w"), Optional.of(CONF_WORKING_DIR));
        JobUtilities.resetWorkingDirectory(this.getConf(), workingDirectory);

        return execute(job, workingDirectory, numTablets);
    }

    public int execute(Job job, String workingDir, int numTabletsCommandLine) throws IOException, InterruptedException, ClassNotFoundException
    {
        final Configuration conf = job.getConfiguration();
        String instanceName = conf.get(TableConstants.CFG_INSTANCE);
        String zooKeepers = conf.get(TableConstants.CFG_ZOOKEEPERS);
        String user = conf.get(TableConstants.CFG_USER);
        String password = conf.get(TableConstants.CFG_PASSWORD);
        final String tableName = conf.get(BitmapConfigHelper.AMINO_BITMAP_BUCKET_TABLE);
        final String temp = IteratorUtils.TEMP_SUFFIX;
        final String tableContext = conf.get("amino.tableContext", "amino");
        final boolean blastIndex = conf.getBoolean("amino.bitmap.first.run", true); //should always assume it's the first run unless specified

        final String splitFile = workingDir + "/bucketSplits.txt";

        Connector c = null;

        boolean success = false;
        try
        {
            final Instance inst = new ZooKeeperInstance(instanceName, zooKeepers);
            c = inst.getConnector(user,password);

            // set number of reducers TODO - Clean this up
            int numReducers = conf.getInt(AMINO_NUM_REDUCERS_BITMAP, 0);
            if (numReducers > 0) {
                job.setNumReduceTasks(numReducers);
            }
            else {
                numReducers = conf.getInt(AMINO_NUM_REDUCERS, DEFAULT_NUM_REDUCERS);
                job.setNumReduceTasks(numReducers);
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
                String tb = tableName + temp;
                if (!blastIndex){
                    tb = tableName;
                }
                String filesPath = workingDir + "/files";
                String failuresPath = workingDir + "/failures";
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
