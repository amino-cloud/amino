package com._42six.amino.bitmap;

import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.util.PathUtils;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.cli.Option;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;

public class StatsJob extends BitmapJob {

	@Override
	public int run(String[] args) throws Exception {
        System.out.println("\n================================ Stats Job ================================\n");

        // Create the command line options to be parsed
        final Option o1 = new Option("o", "outputDir", true, "The output directory");
        initializeConfigAndOptions(args, Optional.of(Sets.newHashSet(o1)));
        final Configuration conf = getConf();
        loadConfigValues(conf);

        Job job = new Job(conf, "Amino stats job");
        job.setJarByClass(StatsJob.class);
        
        job.setInputFormatClass(SequenceFileInputFormat.class);

        String inputPaths = StringUtils.join(PathUtils.getJobDataPaths(conf,
                fromOptionOrConfig(Optional.of("o"), Optional.of(AminoConfiguration.OUTPUT_DIR))), ',');
        System.out.println("Input paths: [" + inputPaths + "].");
        
        String cachePaths = StringUtils.join(PathUtils.getJobCachePaths(conf,
                fromOptionOrConfig(Optional.of("o"), Optional.of(AminoConfiguration.OUTPUT_DIR))), ','); // TODO - Check why same inputPaths
        System.out.println("Cache paths: [" + cachePaths + "].");
        
        PathUtils.setCachePath(job.getConfiguration(), cachePaths);
        SequenceFileInputFormat.setInputPaths(job, inputPaths);
        
        job.setMapperClass(StatsMapper.class);
        job.setMapOutputKeyClass(StatsKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(StatsReducer.class);
        
        // Set number of reducers
        int statsNumReducers = conf.getInt(AminoConfiguration.NUM_REDUCERS_STATS, 0);
        if (statsNumReducers > 0) {
        	job.setNumReduceTasks(statsNumReducers);
        }
        else {
        	job.setNumReduceTasks(conf.getInt(AminoConfiguration.NUM_REDUCERS, AminoConfiguration.DEFAULT_NUM_REDUCERS));
        }
        
        job.setOutputFormatClass(AccumuloOutputFormat.class);

        AccumuloOutputFormat.setZooKeeperInstance(job, new ClientConfiguration().withInstance(instanceName).withZkHosts(zooKeepers));
        AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(password.getBytes("UTF-8")));
        AccumuloOutputFormat.setCreateTables(job, true);

        boolean complete = job.waitForCompletion(true);
        return complete ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new StatsJob(), args));
    }

}
