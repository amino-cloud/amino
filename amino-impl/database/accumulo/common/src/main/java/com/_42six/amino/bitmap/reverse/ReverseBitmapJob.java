package com._42six.amino.bitmap.reverse;

import com._42six.amino.bitmap.BitmapConfigHelper;
import com._42six.amino.bitmap.BitmapJob;
import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.bigtable.TableConstants;
import com._42six.amino.common.util.PathUtils;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.cli.Option;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ReverseBitmapJob extends BitmapJob
{
    private static final String AMINO_NUM_REDUCERS = "amino.num.reducers";
	private static final int DEFAULT_NUM_REDUCERS = 14;
	
	private static boolean recreateTables(Configuration conf) throws IOException {
        String instanceName = conf.get(TableConstants.CFG_INSTANCE);
        String zooKeepers = conf.get(TableConstants.CFG_ZOOKEEPERS);
        String user = conf.get(TableConstants.CFG_USER);
        String password = conf.get(TableConstants.CFG_PASSWORD);
        boolean blastIndex = conf.getBoolean("amino.bitmap.first.run", true); // should always assume it's the first run unless specified
        final String tableContext = conf.get("amino.tableContext", "amino");
        String tableName = conf.get("amino.bitmap.bucketTable");
		tableName = tableName.replace("amino_", "amino_reverse_");

        final TableOperations tableOps = IteratorUtils.connect(instanceName, zooKeepers, user, password).tableOperations();

        int numShards = conf.getInt(BitmapConfigHelper.BITMAP_CONFIG_NUM_SHARDS, 10);
        return IteratorUtils.createTable(tableOps, tableName, tableContext, numShards, blastIndex, blastIndex);
    }

	@Override
	public int run(String[] args) throws Exception 
	{
        System.out.println("\n================================ ReverseBitmapJob ================================\n");

        // Create the command line options to be parsed
        final Option o1 = new Option("o", "outputDir", true, "The output directory");
//        o1.setRequired(true);

        initializeConfigAndOptions(args, Optional.of(Sets.newHashSet(o1)));
        final Configuration conf = getConf();

        final String instanceName = conf.get(TableConstants.CFG_INSTANCE);
        final String zooKeepers = conf.get(TableConstants.CFG_ZOOKEEPERS);
        final String user = conf.get(TableConstants.CFG_USER);
        final String password = conf.get(TableConstants.CFG_PASSWORD);

        if(!recreateTables(conf)){
            return 1;
        }

        final Job job = new Job(conf, "Amino reverse bitmap index job");
        job.setJarByClass(ReverseBitmapJob.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        String inputPaths = StringUtils.join(PathUtils.getJobDataPaths(conf,
                fromOptionOrConfig(Optional.of("o"), Optional.of(CONF_OUTPUT_DIR))), ',');
        System.out.println("Input paths: [" + inputPaths + "].");

        String cachePaths = StringUtils.join(PathUtils.getJobCachePaths(conf,
                fromOptionOrConfig(Optional.of("o"), Optional.of(CONF_OUTPUT_DIR))), ','); // TODO - Check why same as inputPaths
        System.out.println("Cache paths: [" + cachePaths + "].");

        PathUtils.setCachePath(job.getConfiguration(), cachePaths);
        SequenceFileInputFormat.setInputPaths(job, inputPaths);

        job.setMapperClass(ReverseBitmapMapper.class);
        job.setMapOutputKeyClass(ReverseBitmapKey.class);
        //job.setMapOutputValueClass(ReverseBitmapValue.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(ReverseBitmapReducer.class);
        job.setNumReduceTasks(conf.getInt(AMINO_NUM_REDUCERS, DEFAULT_NUM_REDUCERS));
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
