package com._42six.amino.bitmap;

import com._42six.amino.api.framework.FrameworkDriver;
import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.bigtable.TableConstants;
import com._42six.amino.common.util.PathUtils;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;

/**
 * Base class for all of the Jobs related to indexing of the Amino bitmaps
 */
public abstract class BitmapJob extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(BitmapJob.class);

    protected CommandLine commandLine;
    protected Options options = FrameworkDriver.constructGnuOptions();

    protected String instanceName;
    protected String zooKeepers;
    protected String user;
    protected String password;
    protected boolean blastIndex = true; // should always assume it's the first run unless specified
    protected String tableContext = "amino";

    /**
     * Load up the basic configuration values from the Configuration
     *
     * @param conf The Configuration to get the values from
     */
    protected void loadConfigValues(final Configuration conf){
        this.instanceName = Preconditions.checkNotNull(conf.get(TableConstants.CFG_INSTANCE));
        this.zooKeepers = Preconditions.checkNotNull(conf.get(TableConstants.CFG_ZOOKEEPERS));
        this.user = Preconditions.checkNotNull(conf.get(TableConstants.CFG_USER));
        this.password = Preconditions.checkNotNull(conf.get(TableConstants.CFG_PASSWORD));
        this.blastIndex = conf.getBoolean(AminoConfiguration.FIRST_RUN, true); // should always assume it's the first run unless specified
        this.tableContext = conf.get(AminoConfiguration.TABLE_CONTEXT, "amino");
    }

    protected boolean recreateTable(String tableName) throws IOException {
        Preconditions.checkNotNull(instanceName,  "loadConfigs() was not called");

        final TableOperations tableOps = IteratorUtils.connect(instanceName, zooKeepers, user, password).tableOperations();

        return IteratorUtils.createTable(tableOps, tableName, tableContext, blastIndex, blastIndex);
    }

    protected boolean recreateTable(String tableName, int numShards) throws IOException {
        Preconditions.checkNotNull(instanceName,  "loadConfigs() was not called");

        final TableOperations tableOps = IteratorUtils.connect(instanceName, zooKeepers, user, password).tableOperations();

        return IteratorUtils.createTable(tableOps, tableName, tableContext, numShards, blastIndex, blastIndex);
    }


    /**
     * Checks the Configuration and command line parameters for a value.  It first checks the command line to see if the
     * option is there, and if not then checks the Configuration.  If it can't be found, returns null
     *
     * @param optionKey The option from the command line to look for
     * @param configKey The key to look under in the Configuration
     * @param defaultValue   The default value to return if neither are found
     * @return The appropriate value
     */
    protected String fromOptionOrConfig(Optional<String> optionKey, Optional<String> configKey, String defaultValue){
        String retValue = null;

        // First check to see if there is an option to override any config value
        if(optionKey.isPresent()){
            retValue = commandLine.getOptionValue(optionKey.get());
            if(retValue != null){
                logger.info("Getting option '" + optionKey.get() + "' : " + retValue);
                return retValue;
            }
        }

        // Next check to see if the value was in the config
        if(configKey.isPresent()){
            retValue = getConf().get(configKey.get());
            logger.info("Getting value from configuration key '" + configKey.get() + "' : " + retValue);
        }

        return (retValue == null) ? defaultValue : retValue;
    }

    /**
     * Checks the Configuration and command line parameters for a value.  It first checks the command line to see if the
     * option is there, and if not then checks the Configuration.  If it can't be found, returns null
     *
     * @param optionKey The option from the command line to look for
     * @param configKey The key to look under in the Configuration
     * @return The appropriate value or null if not found
     * @throws java.lang.IllegalStateException If neither value is found
     */
    protected String fromOptionOrConfig(Optional<String> optionKey, Optional<String> configKey){
        String retValue = fromOptionOrConfig(optionKey, configKey, null);

        if(retValue == null){
            HelpFormatter help = new HelpFormatter();
            help.printHelp("hadoop jar <jarFile> " + this.getClass().getCanonicalName(), options);
            throw new IllegalStateException("Could not find config value '" + configKey.or("") +
                    "' nor option parameter '" + optionKey.or("") + "'" );
        }

        return retValue;
    }

    /**
     * Processes the command line for any options that might have been passed and loads up the AminoDefaults.xml configuration
     *
     * @param args The command line arguments to parse
     * @param newOptions Any additional options to try and parse in addition to the standard FrameworkDriver options
     * @throws Exception
     */
    protected void initializeConfigAndOptions(String[] args, Optional<HashSet<Option>> newOptions) throws Exception {
        // Add any additional options if present
        if(newOptions.isPresent()) {
            for (Option o : newOptions.get()) {
                options.addOption(o);
            }
        }

        // Parse the arguments and make sure the required args are there
        try{
            commandLine = new GnuParser().parse(options, args);
        } catch (MissingOptionException ex){
            HelpFormatter help = new HelpFormatter();
            help.printHelp("hadoop jar <jarFile> " + this.getClass().getCanonicalName(), options);
            throw ex;
        }

        // Load up the default Amino configurations
        final Configuration conf = getConf();
        if(commandLine.hasOption("base_dir")){
            conf.set(AminoConfiguration.BASE_DIR, commandLine.getOptionValue("base_dir"));
        }
        conf.set(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY, commandLine.getOptionValue("amino_default_config_path"));
        AminoConfiguration.loadAndMergeWithDefault(conf, false);
        AminoConfiguration.createDirConfs(conf);
    }

    /**
     * Initializes all of the common Job parameters
     *
     * @param job The Job to configure
     */
    protected void initializeJob(Job job) throws IOException {
        final Configuration conf = getConf();
        final String outputBaseDir = fromOptionOrConfig(Optional.of("o"), Optional.of(AminoConfiguration.OUTPUT_DIR));
        PathUtils.pathsExists(outputBaseDir, conf);
        final String dataPaths = StringUtils.join(PathUtils.getMultipleJobDataPaths(conf, outputBaseDir), ',');
        final String cachePaths = StringUtils.join(PathUtils.getMultipleJobCachePaths(conf, outputBaseDir), ',');

        System.out.println("Data paths: [" + dataPaths + "].");
        System.out.println("Cache paths: [" + cachePaths + "].");

        PathUtils.setCachePath(job.getConfiguration(), cachePaths);
        SequenceFileInputFormat.setInputPaths(job, dataPaths);

        job.setInputFormatClass(SequenceFileInputFormat.class);
    }

}
