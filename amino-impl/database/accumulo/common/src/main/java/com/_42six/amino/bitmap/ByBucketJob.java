package com._42six.amino.bitmap;


import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.ByBucketKey;
import com._42six.amino.common.JobUtilities;
import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.util.PathUtils;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ConnectorImpl;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.SortedSet;
import java.util.TreeSet;

public class ByBucketJob extends Configured implements Tool {
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
        String bucketTable = conf.get("amino.bitmap.bucketTable");
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
        return  IteratorUtils.createTable(tableOps, bucketTable, numShards, blastIndex, blastIndex);
    }

    public int run(String[] args) throws Exception {

        System.out.println("\n================================ ByBucket Job ================================\n");
        final Configuration conf = getConf();


        if(!recreateTables(conf)){
            return 1;
        }

        final String inputDir = args[0]; // TODO: use configuration instead of positional argument

        final Job job = new Job(conf, "Amino bucket index job");
        job.setJarByClass(ByBucketJob.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        final String inputPaths = StringUtils.join(PathUtils.getJobDataPaths(conf, inputDir), ','); // TODO: use configuration instead of positional argument
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
        final String tableName = conf.get(BitmapConfigHelper.AMINO_BITMAP_BUCKET_TABLE);
        final String temp = IteratorUtils.TEMP_SUFFIX;
        final boolean blastIndex = conf.getBoolean("amino.bitmap.first.run", true); //should always assume it's the first run unless specified

        final String splitFile = workingDir + "/bucketSplits.txt";

        ConnectorImpl c = null;

        boolean success = false;
        try
        {
            final Instance inst = new ZooKeeperInstance(instanceName, zooKeepers);
            final AuthInfo ai = new AuthInfo();
            ai.setUser(user);
            ai.setPassword(password.getBytes());
            c = new ConnectorImpl(inst, ai.getUser(), ai.getPassword());

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
            for (int i = 1; i <= numReducers + 1; i++)
            {
                Text split = new Text(Integer.toString(i));
                out.println(new String(Base64.encodeBase64(TextUtil.getBytes(split))));
                System.out.println("Split: " + i);
            }
            out.flush();
            out.close();

            success = IteratorUtils.createTable(c.tableOperations(), tableName, splits, blastIndex, blastIndex);

            job.setOutputFormatClass(AccumuloFileOutputFormat.class);

            AccumuloFileOutputFormat.setOutputPath(job, new Path(workingDir + "/files"));

            //job.setPartitionerClass(KeyRangePartitioner.class);
            //KeyRangePartitioner.setSplitFile(job, splitFile);
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
                System.out.println("Importing the files in '" + workingDir + "/files' to the table: " + tb);
                c.tableOperations().importDirectory(tb, workingDir + "/files", workingDir + "/failures", 20, 4, false);
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
        Configuration conf = new Configuration();
        conf.set(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY, args[1]); // TODO: use flag instead of positional
        AminoConfiguration.loadDefault(conf, "AminoDefaults", true);

        int res = ToolRunner.run(conf, new ByBucketJob(), args);
        System.exit(res);
    }

}
