package com._42six.amino.bitmap.reverse;

import com._42six.amino.bitmap.BitmapConfigHelper;
import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.JobUtilities;
import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.util.PathUtils;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.KeyRangePartitioner;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
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
import java.io.PrintStream;
import java.util.SortedSet;
import java.util.TreeSet;

public class ReverseFeatureLookupJob extends Configured implements Tool
{
    private static final String BIGTABLE_INSTANCE = "bigtable.instance";
    private static final String BIGTABLE_ZOOKEEPERS = "bigtable.zookeepers";
    private static final String BIGTABLE_USERNAME = "bigtable.username";
    private static final String BIGTABLE_PASSWORD = "bigtable.password";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY, args[1]); // TODO: use flag instead of positional
        AminoConfiguration.loadDefault(conf, "AminoDefaults", true);

        int res = ToolRunner.run(conf, new ReverseFeatureLookupJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception
    {
        System.out.println("\n================================ ReverseFeatureLookupJob ================================\n");
        final Configuration conf = getConf();

        final Job job = new Job(conf, "Amino reverse_feature_lookup table job");
        job.setJarByClass(ReverseFeatureLookupJob.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // Parse the command line parameters
        final String inputPaths = StringUtils.join(PathUtils.getJobDataPaths(conf, args[0]), ',');
        final String cachePaths = StringUtils.join(PathUtils.getJobCachePaths(conf, args[0]), ',');
        final String workingDirectory = args[2]; // TODO: use configuration instead of positional argument

        System.out.println("Input paths: [" + inputPaths + "].");
        System.out.println("Cache paths: [" + cachePaths + "].");

        JobUtilities.deleteDirectory(conf, workingDirectory);
        PathUtils.setCachePath(job.getConfiguration(), cachePaths);
        SequenceFileInputFormat.setInputPaths(job, inputPaths);

        // Configure the mapper
        job.setMapperClass(ReverseFeatureLookupMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);

        // Configure the reducer
        // Use the IdentityReducer - no need to call job.setReducerClass()
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Value.class);

        final int numberOfShards = conf.getInt(BitmapConfigHelper.BITMAP_CONFIG_NUM_SHARDS, 10);
        job.setNumReduceTasks(numberOfShards);

        // Grab params for connecting to BigTable instance
        final String instanceName = conf.get(BIGTABLE_INSTANCE);
        final String zooKeepers = conf.get(BIGTABLE_ZOOKEEPERS);
        final String user = conf.get(BIGTABLE_USERNAME);
        final String password = conf.get(BIGTABLE_PASSWORD);
        final String tableName = conf.get("amino.bitmap.featureLookupTable").replace("amino_", "amino_reverse_");

        boolean blastIndex = conf.getBoolean("amino.bitmap.first.run", true); // should always assume it's the first run unless specified

        Connector connector = null;
        PrintStream splitsPrinter = null;

        boolean success = false;
        final FileSystem fs = FileSystem.get(conf);

        // Create the splits for the splitfile
        final SortedSet<Text> splits = new TreeSet<Text>();
        for (int shard = 0; shard < numberOfShards; shard++)
        {
            splits.add(new Text(Integer.toString(shard)));
        }

        try
        {
            final Instance instance = new ZooKeeperInstance(instanceName, zooKeepers);
            connector = instance.getConnector(user, password);

            splitsPrinter = new PrintStream(new BufferedOutputStream(fs.create(new Path(workingDirectory + "/splits.txt"))));
            for (Text split : splits)
            {
                splitsPrinter.println(new String(Base64.encodeBase64(TextUtil.getBytes(split))));
            }
            splitsPrinter.flush();
            splitsPrinter.close();

            success = IteratorUtils.createTable(connector.tableOperations(), tableName, splits, blastIndex, blastIndex);

            job.setOutputFormatClass(AccumuloFileOutputFormat.class);
            AccumuloFileOutputFormat.setOutputPath(job, new Path(workingDirectory + "/files"));
            job.setPartitionerClass(KeyRangePartitioner.class);
            RangePartitioner.setSplitFile(job, workingDirectory + "/splits.txt");
        }
        catch (AccumuloException e)
        {
            e.printStackTrace();
        }
        catch (AccumuloSecurityException e)
        {
            e.printStackTrace();
        }
        finally {
            if(splitsPrinter != null){
                splitsPrinter.close();
            }
        }

        int result = 0;
        if (success)
        {
            result = job.waitForCompletion(true) ? 0 : -1;
        }

        // Bulk import the results into the database
        if (connector != null && success)
        {
            System.out.println("Importing job results to accumulo");
            try
            {
                final String importTable = (!blastIndex) ? tableName : tableName + IteratorUtils.TEMP_SUFFIX;
                connector.tableOperations().importDirectory(importTable, workingDirectory + "/files", workingDirectory + "/failures", false);
                result = JobUtilities.failureDirHasFiles(conf, workingDirectory + "/failures");
            }
            catch (Exception e)
            {
                result = 1;
                e.printStackTrace();
            }
        }

        return result;
    }

}
