package com._42six.amino.bitmap;

import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.bigtable.TableConstants;
import com._42six.amino.common.util.PathUtils;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StatsJob extends Configured implements Tool {

    private static final String AMINO_NUM_REDUCERS = "amino.num.reducers";
    private static final String AMINO_NUM_REDUCERS_STATS = "amino.num.reducers.job.stats";
    
	private static final int DEFAULT_NUM_REDUCERS = 14;
	
//	private static void recreateLookupTable(Configuration conf) throws IOException {
//        AminoConfiguration.loadDefault(conf, "AminoDefaults");
//        String instanceName = conf.get(ACCUMULO_INSTANCE);
//        String zooKeepers = conf.get(ACCUMULO_ZOOKEEPERS);
//        String user = conf.get(ACCUMULO_USERNAME);
//        String password = conf.get(ACCUMULO_PASSWORD);
//        String lookupTable = conf.get("amino.bitmap.featureLookupTable");
//
//        Instance inst = new ZooKeeperInstance(instanceName, zooKeepers);
//        TableOperations tableOps;
//        try {
//            tableOps = inst.getConnector(user, password).tableOperations();
//        } catch (BTException ex) {
//            throw new IOException(ex);
//        } catch (BTSecurityException ex) {
//            throw new IOException(ex);
//        }
//
//        IteratorUtils.createTable(tableOps, lookupTable, true, true);
//    }

	@Override
	public int run(String[] args) throws Exception {
        System.out.println("\n================================ Stats Job ================================\n");
		Configuration conf = getConf();
        String instanceName = conf.get(TableConstants.CFG_INSTANCE);
        String zooKeepers = conf.get(TableConstants.CFG_ZOOKEEPERS);
        String user = conf.get(TableConstants.CFG_USER);
        String password = conf.get(TableConstants.CFG_PASSWORD);
        
        //recreateLookupTable(conf);

        Job job = new Job(conf, "Amino stats job");
        job.setJarByClass(StatsJob.class);
        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        
        
        String inputPaths = StringUtils.join(PathUtils.getJobDataPaths(conf, args[0]), ',');
        System.out.println("Input paths: [" + inputPaths + "].");
        
        String cachePaths = StringUtils.join(PathUtils.getJobCachePaths(conf, args[0]), ',');
        System.out.println("Cache paths: [" + cachePaths + "].");
        
        PathUtils.setCachePath(job.getConfiguration(), cachePaths);
        SequenceFileInputFormat.setInputPaths(job, inputPaths); // TODO: use configuration instead of positional argument
        
        job.setMapperClass(StatsMapper.class);
        job.setMapOutputKeyClass(StatsKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(StatsReducer.class);
        
        //set number of reducers
        int statsNumReducers = conf.getInt(AMINO_NUM_REDUCERS_STATS, 0);
        if (statsNumReducers > 0) {
        	job.setNumReduceTasks(statsNumReducers);
        }
        else {
        	job.setNumReduceTasks(conf.getInt(AMINO_NUM_REDUCERS, DEFAULT_NUM_REDUCERS));
        }
        
        job.setOutputFormatClass(AccumuloOutputFormat.class);
        AccumuloOutputFormat.setZooKeeperInstance(job, instanceName, zooKeepers);
        AccumuloOutputFormat.setOutputInfo(job, user, password.getBytes(), true, null);
//        AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(password));
//        AccumuloOutputFormat.setCreateTables(job, true);
//        AccumuloOutputFormat.setDefaultTableName(job, null);

        boolean complete = job.waitForCompletion(true);
        if (!complete)
		{
			return 1;
		}
		else
		{
			return 0;
		}
	}
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY, args[1]); // TODO: use flag instead of positional
        AminoConfiguration.loadDefault(conf, "AminoDefaults", true);
        
        int res = ToolRunner.run(conf, new StatsJob(), args);
        System.exit(res);
    }

}
