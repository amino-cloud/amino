package com._42six.amino.bitmap.reverse;

import com._42six.amino.bitmap.BitmapJob;
import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.JobUtilities;
import com._42six.amino.common.accumulo.IteratorUtils;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.KeyRangePartitioner;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
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
import java.io.PrintStream;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Processes the values in the output directory and bulk inserts them to the amino reverse feature table
 */
public class ReverseFeatureLookupJob extends BitmapJob
{
    @Override
    public int run(String[] args) throws Exception
    {
        System.out.println("\n================================ ReverseFeatureLookupJob ================================\n");
        // Create the command line options to be parsed
        final Option o1 = new Option("o", "outputDir", true, "The output directory");
        final Option o2 = new Option("w", "workingDir", true, "The working directory");

        initializeConfigAndOptions(args, Optional.of(Sets.newHashSet(o1, o2)));
        final Configuration conf = getConf();

        final Job job = new Job(conf, "Amino reverse_feature_lookup table job");
        job.setJarByClass(ReverseFeatureLookupJob.class);
        final String workingDirectory = fromOptionOrConfig(Optional.of("w"), Optional.of(AminoConfiguration.WORKING_DIR));
        JobUtilities.resetWorkingDirectory(this.getConf(), workingDirectory);

        initializeJob(job);

        // Configure the mapper
        job.setMapperClass(ReverseFeatureLookupMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);

        // Configure the reducer
        // Use the IdentityReducer - no need to call job.setReducerClass()
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Value.class);

        final int numberOfShards = conf.getInt(AminoConfiguration.NUM_SHARDS, 10);
        job.setNumReduceTasks(numberOfShards);

        // Grab params for connecting to BigTable instance
        loadConfigValues(conf);
        final String tableName = conf.get(AminoConfiguration.TABLE_FEATURE_LOOKUP).replace("amino_", "amino_reverse_");

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
            connector = instance.getConnector(user, new PasswordToken(password));

            splitsPrinter = new PrintStream(new BufferedOutputStream(fs.create(new Path(workingDirectory + "/splits.txt"))));
            for (Text split : splits)
            {
                splitsPrinter.println(new String(Base64.encodeBase64(TextUtil.getBytes(split))));
            }
            splitsPrinter.flush();
            splitsPrinter.close();

            success = IteratorUtils.createTable(connector.tableOperations(), tableName, tableContext, splits, blastIndex, blastIndex);

            job.setOutputFormatClass(AccumuloFileOutputFormat.class);
            AccumuloFileOutputFormat.setOutputPath(job, new Path(workingDirectory + "/files"));
            job.setPartitionerClass(KeyRangePartitioner.class);
            RangePartitioner.setSplitFile(job, workingDirectory + "/splits.txt");
        }
        catch (AccumuloException | AccumuloSecurityException e)
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
            System.out.println("Importing job results to Accumulo");
            try
            {
                final String importTable = (!blastIndex) ? tableName : tableName + AminoConfiguration.TEMP_SUFFIX;
                JobUtilities.setGroupAndPermissions(conf, workingDirectory);
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

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ReverseFeatureLookupJob(), args));
    }

}
