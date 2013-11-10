package com._42six.amino.bitmap.reverse;


import com._42six.amino.bitmap.BitmapConfigHelper;
import com._42six.amino.bitmap.HypothesisKeyComparator;
import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.JobUtilities;
import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.util.PathUtils;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.impl.ConnectorImpl;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
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

public class ReverseHypothesisJob extends Configured implements Tool
{
	private static final String ACCUMULO_INSTANCE = "bigtable.instance";
    private static final String ACCUMULO_ZOOKEEPERS = "bigtable.zookeepers";
    private static final String ACCUMULO_USERNAME = "bigtable.username";
    private static final String ACCUMULO_PASSWORD = "bigtable.password";
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY, args[1]); // TODO: use flag instead of positional
        AminoConfiguration.loadDefault(conf, "AminoDefaults", true);
        
        int res = ToolRunner.run(conf, new ReverseHypothesisJob(), args);
        System.exit(res);
    }

	@Override
	public int run(String[] args) throws Exception 
	{
        System.out.println("\n================================ ReverseHypothesisJob ================================\n");
		Configuration conf = getConf();

        Job job = new Job(conf, "Amino reverse hypothesis table job");
        job.setJarByClass(ReverseHypothesisJob.class);
        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        String inputPaths = StringUtils.join(PathUtils.getJobDataPaths(conf, args[0]), ',');
        System.out.println("Input paths: [" + inputPaths + "].");
        
        String cachePaths = StringUtils.join(PathUtils.getJobCachePaths(conf, args[0]), ',');
        System.out.println("Cache paths: [" + cachePaths + "].");
        
        PathUtils.setCachePath(job.getConfiguration(), cachePaths);
        SequenceFileInputFormat.setInputPaths(job, inputPaths); 
        
        job.setMapperClass(ReverseHypothesisMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ReverseHypothesisValue.class);

        job.setReducerClass(ReverseHypothesisReducer.class); 
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Value.class);
        
        String workingDirectory = args[2]; // TODO: use configuration instead of positional argument
        JobUtilities.deleteDirectory(this.getConf(), workingDirectory);
        
        return execute(job, workingDirectory);
	}
	
	public int execute(Job job, String workingDir) throws IOException, InterruptedException, ClassNotFoundException, TableNotFoundException, TableExistsException
	{
		Configuration conf = job.getConfiguration();
		//AminoConfiguration.loadDefault(conf, "AminoDefaults", true);
		String instanceName = conf.get(ACCUMULO_INSTANCE);
        String zooKeepers = conf.get(ACCUMULO_ZOOKEEPERS);
        String user = conf.get(ACCUMULO_USERNAME);
        String password = conf.get(ACCUMULO_PASSWORD);
        String tableName = conf.get("amino.bitmap.featureLookupTable");
        tableName = tableName.replace("amino_", "amino_reverse_");
        //String tableName = "bucketValueLookup";
        String temp = IteratorUtils.TEMP_SUFFIX;
        boolean blastIndex = conf.getBoolean("amino.bitmap.first.run", true); //should always assume it's the first run unless specified
        
        ConnectorImpl c = null;
        PrintStream out;
        boolean success = false;
        try
        {
        	Instance inst = new ZooKeeperInstance(instanceName, zooKeepers);
        	AuthInfo ai = new AuthInfo();
        	ai.setUser(user);
        	ai.setPassword(password.getBytes());
        	c = new ConnectorImpl(inst, ai.getUser(), ai.getPassword());
 
        	final int numberOfShards = conf.getInt(BitmapConfigHelper.BITMAP_CONFIG_NUM_SHARDS, 10);
        	//final int numberOfHashes = conf.getInt("amino.bitmap.num-hashes", 1);
        	SortedSet<Text> splits = new TreeSet<Text>();

        	for (int shard = 1; shard < numberOfShards; shard++)
        	{
        		splits.add(new Text(Integer.toString(shard)));
        	}
        	job.setNumReduceTasks(numberOfShards);

        	FileSystem fs = FileSystem.get(conf);
        	out = new PrintStream(new BufferedOutputStream(fs.create(new Path(workingDir + "/splits.txt"))));
        	for (Text split : splits)
        	{
        		out.println(new String(Base64.encodeBase64(TextUtil.getBytes(split))));
        	}
        	out.flush();
        	out.close();

        	success = IteratorUtils.createTable(c.tableOperations(), tableName, splits, blastIndex, blastIndex);
        	
        	job.setOutputFormatClass(AccumuloFileOutputFormat.class);
        	AccumuloFileOutputFormat.setOutputPath(job, new Path(workingDir + "/files"));
        	job.setPartitionerClass(RangePartitioner.class);
        	job.setSortComparatorClass(HypothesisKeyComparator.class); //This will ensure the values come in sorted so we don't have to do that TreeMap...
            RangePartitioner.setSplitFile(job, workingDir + "/splits.txt");

//            job.setOutputFormatClass(AccumuloOutputFormat.class);
//            AccumuloOutputFormat.setZooKeeperInstance(job, instanceName, zooKeepers);
//            AccumuloOutputFormat.setOutputInfo(job, user, password.getBytes(), true, null);


        	//success = true;
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
        
        if (c != null && success)
        {
        	System.out.println("Importing job results to accumulo");
        	try
        	{
        		String tb = tableName + temp;
        		if (!blastIndex){
                    tb = tableName;
                }
        		c.tableOperations().importDirectory(tb, workingDir + "/files", workingDir + "/failures", 20, 4, false);
        		result = JobUtilities.failureDirHasFiles(conf, workingDir + "/failures");
        	}
        	
        	catch (IOException e)
			{
				result = 1;
				e.printStackTrace();
			} 
        	catch (AccumuloException e) 
			{
				e.printStackTrace();
			} catch (AccumuloSecurityException e) 
			{
				e.printStackTrace();
			}
        }
        
        return result;
	}

}
