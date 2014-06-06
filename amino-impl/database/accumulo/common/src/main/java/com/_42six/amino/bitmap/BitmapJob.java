package com._42six.amino.bitmap;

import com._42six.amino.api.framework.FrameworkDriver;
import com._42six.amino.common.AminoConfiguration;
import com.google.common.base.Optional;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.util.HashSet;

/**
 * Base class for all of the Jobs related to indexing of the Amino bitmaps
 */
public abstract class BitmapJob extends Configured implements Tool {
    protected CommandLine commandLine;
    protected Options options = FrameworkDriver.constructGnuOptions();

    public static final String CONF_OUTPUT_DIR = "amino.output"; // TODO Roll this into a central configuration class
    public static final String CONF_WORKING_DIR = "amino.working"; // TODO Roll this into a central configuration class
    public static final String CONF_CACHE_DIR = "amino.cache"; // TODO Roll this into a central configuration class

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
                return retValue;
            }
        }

        // Next check to see if the value was in the config
        if(configKey.isPresent()){
            retValue = getConf().get(configKey.get());
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
        conf.set(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY, commandLine.getOptionValue("amino_default_config_path"));
        AminoConfiguration.loadDefault(conf, "AminoDefaults", false);
    }
}
