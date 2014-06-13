package com._42six.amino.bitmap.reverse;

import com._42six.amino.bitmap.BitmapJob;
import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.util.PathUtils;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.cli.Option;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;

public class ReverseBitmapJob extends BitmapJob
{
	@Override
	public int run(String[] args) throws Exception 
	{
        System.out.println("\n================================ ReverseBitmapJob ================================\n");

        // Create the command line options to be parsed
        final Option o1 = new Option("o", "outputDir", true, "The output directory");

        initializeConfigAndOptions(args, Optional.of(Sets.newHashSet(o1)));
        final Configuration conf = getConf();
        loadConfigValues(conf);
        String tableName = Preconditions.checkNotNull(conf.get(AminoConfiguration.TABLE_BUCKET), "bucketTable config value missing");
        tableName = tableName.replace("amino_", "amino_reverse_");
        final int numShards = conf.getInt(AminoConfiguration.NUM_SHARDS, 10);

        if(!recreateTable(tableName, numShards)){
            return 1;
        }

        final Job job = new Job(conf, "Amino reverse bitmap index job");
        job.setJarByClass(ReverseBitmapJob.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        String inputPaths = StringUtils.join(PathUtils.getJobDataPaths(conf,
                fromOptionOrConfig(Optional.of("o"), Optional.of(AminoConfiguration.OUTPUT_DIR))), ',');
        System.out.println("Input paths: [" + inputPaths + "].");

        String cachePaths = StringUtils.join(PathUtils.getJobCachePaths(conf,
                fromOptionOrConfig(Optional.of("o"), Optional.of(AminoConfiguration.OUTPUT_DIR))), ','); // TODO - Check why same as inputPaths
        System.out.println("Cache paths: [" + cachePaths + "].");

        PathUtils.setCachePath(job.getConfiguration(), cachePaths);
        SequenceFileInputFormat.setInputPaths(job, inputPaths);

        job.setMapperClass(ReverseBitmapMapper.class);
        job.setMapOutputKeyClass(ReverseBitmapKey.class);
        //job.setMapOutputValueClass(ReverseBitmapValue.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(ReverseBitmapReducer.class);
        job.setNumReduceTasks(conf.getInt(AminoConfiguration.NUM_REDUCERS, AminoConfiguration.DEFAULT_NUM_REDUCERS));
        job.setOutputFormatClass(AccumuloOutputFormat.class);

        AccumuloOutputFormat.setZooKeeperInstance(job, new ClientConfiguration().withInstance(instanceName).withZkHosts(zooKeepers));
        AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(password.getBytes("UTF-8")));
        AccumuloOutputFormat.setCreateTables(job, true);

        return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ReverseBitmapJob(), args));
    }

}
