package com._42six.amino.bitmap;

import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.util.PathUtils;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class BitmapJob extends Configured implements Tool {
    private static final String ACCUMULO_INSTANCE = "bigtable.instance";
    private static final String ACCUMULO_ZOOKEEPERS = "bigtable.zookeepers";
    private static final String ACCUMULO_USERNAME = "bigtable.username";
    private static final String ACCUMULO_PASSWORD = "bigtable.password";
    
    private static final String AMINO_NUM_REDUCERS = "amino.num.reducers";
    private static final String AMINO_NUM_REDUCERS_BITMAP = "amino.num.reducers.job.bitmap";
	private static final int DEFAULT_NUM_REDUCERS = 14;

    private static boolean recreateTables(Configuration conf) throws IOException {
        //AminoConfiguration.loadDefault(conf, "AminoDefaults", true);
        String instanceName = conf.get(ACCUMULO_INSTANCE);
        String zooKeepers = conf.get(ACCUMULO_ZOOKEEPERS);
        String user = conf.get(ACCUMULO_USERNAME);
        String password = conf.get(ACCUMULO_PASSWORD);
        String bucketTable = conf.get("amino.bitmap.bucketTable");
        String indexTable = conf.get("amino.bitmap.indexTable");
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
        boolean success = IteratorUtils.createTable(tableOps, bucketTable, numShards, blastIndex, blastIndex);
        if(success) {
            success = IteratorUtils.createTable(tableOps, indexTable, blastIndex, blastIndex);
        }
        return success;
    }

    public int run(String[] args) throws Exception {
        System.out.println("\n================================ Bitmap Job ================================\n");

        Configuration conf = getConf();
        //AminoConfiguration.loadDefault(conf, "AminoDefaults", true);
        String instanceName = conf.get(ACCUMULO_INSTANCE);
        String zooKeepers = conf.get(ACCUMULO_ZOOKEEPERS);
        String user = conf.get(ACCUMULO_USERNAME);
        String password = conf.get(ACCUMULO_PASSWORD);
                
        boolean complete = recreateTables(conf);

        if (complete)
        {
	        Job job = new Job(conf, "Amino bitmap index job");
	        job.setJarByClass(BitmapJob.class);
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	        
	        String inputPaths = StringUtils.join(PathUtils.getJobDataPaths(conf, args[0]), ','); // TODO: use configuration instead of positional argument
	        System.out.println("Input paths: [" + inputPaths + "].");
	        
	        String cachePaths = StringUtils.join(PathUtils.getJobCachePaths(conf, args[0]), ',');
	        System.out.println("Cache paths: [" + cachePaths + "].");
	        
	        PathUtils.setCachePath(job.getConfiguration(), cachePaths);
	        SequenceFileInputFormat.setInputPaths(job, inputPaths); 
	        job.setMapperClass(BitmapMapper.class);
	        job.setMapOutputKeyClass(BitmapKey.class);
	        job.setMapOutputValueClass(BitmapValue.class);
	        job.setCombinerClass(BitmapCombiner.class);
	        job.setReducerClass(BitmapReducer.class);
	        
	        //set number of reducers
	        int bitmapNumReducers = conf.getInt(AMINO_NUM_REDUCERS_BITMAP, 0);
	        if (bitmapNumReducers > 0) {
	        	job.setNumReduceTasks(bitmapNumReducers);
	        }
	        else {
	        	job.setNumReduceTasks(conf.getInt(AMINO_NUM_REDUCERS, DEFAULT_NUM_REDUCERS));
	        }
	        
	        job.setOutputFormatClass(AccumuloOutputFormat.class);
            AccumuloOutputFormat.setZooKeeperInstance(job, instanceName, zooKeepers);
            AccumuloOutputFormat.setOutputInfo(job, user, password.getBytes(), true, null);
	
	        complete = job.waitForCompletion(true);
        }
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
        
        int res = ToolRunner.run(conf, new BitmapJob(), args);
        System.exit(res);
    }
}
