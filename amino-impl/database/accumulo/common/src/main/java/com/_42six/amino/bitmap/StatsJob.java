package com._42six.amino.bitmap;

import com._42six.amino.common.bigtable.TableConstants;
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
        // Create the command line options to be parsed
        final Option o1 = new Option("o", "outputDir", true, "The output directory");
//        o1.setRequired(true);
        initializeConfigAndOptions(args, Optional.of(Sets.newHashSet(o1)));
        final Configuration conf = getConf();

        final String instanceName = conf.get(TableConstants.CFG_INSTANCE);
        final String zooKeepers = conf.get(TableConstants.CFG_ZOOKEEPERS);
        final String user = conf.get(TableConstants.CFG_USER);
        final String password = conf.get(TableConstants.CFG_PASSWORD);
        
        // recreateLookupTable(conf);

        Job job = new Job(conf, "Amino stats job");
        job.setJarByClass(StatsJob.class);
        
        job.setInputFormatClass(SequenceFileInputFormat.class);

        String inputPaths = StringUtils.join(PathUtils.getJobDataPaths(conf,
                fromOptionOrConfig(Optional.of("o"), Optional.of(CONF_OUTPUT_DIR))), ',');
        System.out.println("Input paths: [" + inputPaths + "].");
        
        String cachePaths = StringUtils.join(PathUtils.getJobCachePaths(conf,
                fromOptionOrConfig(Optional.of("o"), Optional.of(CONF_OUTPUT_DIR))), ','); // TODO - Check why same inputPaths
        System.out.println("Cache paths: [" + cachePaths + "].");
        
        PathUtils.setCachePath(job.getConfiguration(), cachePaths);
        SequenceFileInputFormat.setInputPaths(job, inputPaths);
        
        job.setMapperClass(StatsMapper.class);
        job.setMapOutputKeyClass(StatsKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(StatsReducer.class);
        
        // Set number of reducers
        int statsNumReducers = conf.getInt(AMINO_NUM_REDUCERS_STATS, 0);
        if (statsNumReducers > 0) {
        	job.setNumReduceTasks(statsNumReducers);
        }
        else {
        	job.setNumReduceTasks(conf.getInt(AMINO_NUM_REDUCERS, DEFAULT_NUM_REDUCERS));
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
