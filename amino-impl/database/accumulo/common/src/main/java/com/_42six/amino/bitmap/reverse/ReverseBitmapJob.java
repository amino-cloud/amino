package com._42six.amino.bitmap.reverse;

import com._42six.amino.bitmap.BitmapConfigHelper;
import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.util.PathUtils;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ReverseBitmapJob extends Configured implements Tool  
{
	private static final String ACCUMULO_INSTANCE = "bigtable.instance";
    private static final String ACCUMULO_ZOOKEEPERS = "bigtable.zookeepers";
    private static final String ACCUMULO_USERNAME = "bigtable.username";
    private static final String ACCUMULO_PASSWORD = "bigtable.password";
    
    private static final String AMINO_NUM_REDUCERS = "amino.num.reducers";
	private static final int DEFAULT_NUM_REDUCERS = 14;
	
	private static boolean recreateTables(Configuration conf) throws IOException {
        //AminoConfiguration.loadDefault(conf, "AminoDefaults", true);
        String instanceName = conf.get(ACCUMULO_INSTANCE);
        String zooKeepers = conf.get(ACCUMULO_ZOOKEEPERS);
        String user = conf.get(ACCUMULO_USERNAME);
        String password = conf.get(ACCUMULO_PASSWORD);
        boolean blastIndex = conf.getBoolean("amino.bitmap.first.run", true); // should always assume it's the first run unless specified
        String tableName = conf.get("amino.bitmap.bucketTable");
		tableName = tableName.replace("amino_", "amino_reverse_");

        final TableOperations tableOps = IteratorUtils.connect(instanceName, zooKeepers, user, password).tableOperations();

        int numShards = conf.getInt(BitmapConfigHelper.BITMAP_CONFIG_NUM_SHARDS, 10);
        boolean success = IteratorUtils.createTable(tableOps, tableName, numShards, blastIndex, blastIndex);
        
        return success;
    }

	@Override
	public int run(String[] args) throws Exception 
	{
        System.out.println("\n================================ ReverseBitmapJob ================================\n");
		final Configuration conf = getConf();
        //AminoConfiguration.loadDefault(conf, "AminoDefaults", true);
        String instanceName = conf.get(ACCUMULO_INSTANCE);
        String zooKeepers = conf.get(ACCUMULO_ZOOKEEPERS);
        String user = conf.get(ACCUMULO_USERNAME);
        String password = conf.get(ACCUMULO_PASSWORD);
                
        boolean complete = recreateTables(conf);

        if (complete)
        {
	        final Job job = new Job(conf, "Amino reverse bitmap index job");
	        job.setJarByClass(ReverseBitmapJob.class);
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	        
	        String inputPaths = StringUtils.join(PathUtils.getJobDataPaths(conf, args[0]), ',');
	        System.out.println("Input paths: [" + inputPaths + "].");
	        
	        String cachePaths = StringUtils.join(PathUtils.getJobCachePaths(conf, args[0]), ',');
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
            AccumuloOutputFormat.setZooKeeperInstance(job, instanceName, zooKeepers);
//            AccumuloOutputFormat.setOutputInfo(job, user, password.getBytes(), true, null);
            AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(password));
            AccumuloOutputFormat.setCreateTables(job, true);
            AccumuloOutputFormat.setDefaultTableName(job, null);
	
	        complete = job.waitForCompletion(true);
        }

        return complete ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY, args[1]); // TODO: use flag instead of positional
        AminoConfiguration.loadDefault(conf, "AminoDefaults", true);
        
        int res = ToolRunner.run(conf, new ReverseBitmapJob(), args);
        System.exit(res);
    }

}
