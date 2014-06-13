package com._42six.amino.common;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map.Entry;

public class AminoConfiguration extends Configuration {

    public static final String TEMP_SUFFIX = "_temp";
    public static final String OLD_SUFFIX = "old";  // TODO - is this used?

    public static final String DEFAULT_CONFIG_CLASS_NAME = "AminoDefaults";
	public static final String DEFAULT_CONFIGURATION_PATH_KEY = "amino.default.configuration.path";

    public static final int DEFAULT_NUM_REDUCERS = 14;

    public static final String FIRST_RUN = "amino.bitmap.first.run";
    public static final String TABLE_CONTEXT = "amino.tableContext";

    public static final String NUM_HASHES = "amino.bitmap.num-hashes";
    public static final String NUM_SHARDS = "amino.bigtable.number.of.shards";
    public static final String NUM_SHARDS_HYPOTHESIS = "amino.bigtable.number.of.shards.hypothesis";
    public static final String NUM_REDUCERS = "amino.num.reducers";

    public static final String NUM_REDUCERS_BITMAP = "amino.num.reducers.job.bitmap";
    public static final String NUM_REDUCERS_STATS = "amino.num.reducers.job.stats";

    public static final String OUTPUT_DIR = "amino.output";
    public static final String WORKING_DIR = "amino.working";
    public static final String CACHE_DIR = "amino.cache";

    public static final String JOB_NAME = "amino.bitmap.job.name";
    public static final String INPUT_PATH = "amino.bitmap.input.path";

    public static final String MAX_MEMORY = "bigtable.maxMemory";
    public static final String MAX_LATENCY = "bigtable.maxLatency";
    public static final String MAX_WRITE_THREADS = "bigtable.maxWriteThreads";

    // Tables
    public static final String TABLE_BUCKET = "amino.bitmap.bucketTable";
    public static final String TABLE_INDEX = "amino.bitmap.indexTable";
    public static final String TABLE_FEATURE_LOOKUP = "amino.bitmap.featureLookupTable";
    public static final String TABLE_HYPOTHESIS = "amino.hypothesisTable";
    public static final String TABLE_RESULT = "amino.queryResultTable";
    public static final String TABLE_GROUP_MEMBERSHIP = "amino.groupMembershipTable";
    public static final String TABLE_GROUP_HYPOTHESIS_LOOKUP = "amino.groupHypothesisLUT";
    public static final String TABLE_GROUP_METADATA = "amino.groupMetadataTable";
    public static final String TABLE_METADATA = "amino.metadataTable";


//    private String defaultDirectory;

//    public AminoConfiguration() {
//        this(DEFAULT_CONFIGURATION_PATH_KEY);
//    }
//
//    public AminoConfiguration(String defaultDirectory){
//        this(defaultDirectory, false);
//    }
//
//    public AminoConfiguration(String defaultDirectory, boolean loadAminoDefaults) {
//        this.defaultDirectory = defaultDirectory;
//        this.set(DEFAULT_CONFIGURATION_PATH_KEY, defaultDirectory);
//
//        if(loadAminoDefaults){
//            try {
//                AminoConfiguration.loadAndMergeWithDefault(this, true);
//            } catch (IOException e) {
//                e.printStackTrace();
//                throw new RuntimeException(e); // TODO - This prob isn't the best
//            }
//        }
//    }


    public static void loadAndMergeWithDefault(Configuration conf, boolean overrideValues) throws IOException {
        loadAndMerge(conf, DEFAULT_CONFIG_CLASS_NAME, overrideValues);
    }

	/**
	 * Loads properties from file and merges into conf.
	 * TODO: Note that this allows anyone running a job to overwrite already-set
	 * config files in Hadoop.  When we eventually have different devs using
	 * Amino on the same machine to run different jobs, we should ensure that they
	 * can't overwrite any Hadoop property.
	 * 
	 * @param conf current properties
	 * @param className The name of the class to look for the config for.  This is will look for a configuration file
     *                  with the same name as the class
	 * @param overrideValues true to overwrite already-set properties in conf
	 * @throws IOException
	 */
	public static void loadAndMerge(Configuration conf, String className, boolean overrideValues)
			throws IOException {
        Preconditions.checkNotNull(conf);
        MorePreconditions.checkNotNullOrEmpty(className);

		final String defaultConfigurationPath = conf
				.get(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY);

		// Make sure that we have a valid Amino Object
		if (defaultConfigurationPath == null) {
			throw new IllegalArgumentException(
					"This configuration does not have the default configuration path for Amino specified!");
		}

		final FileSystem fs = FileSystem.get(conf);
		final Path defaultConfigFilePath = new Path(String.format("%s/%s.xml", defaultConfigurationPath, className));

		if (!fs.exists(defaultConfigFilePath)) {
			throw new IOException("File " + defaultConfigFilePath.toString()
					+ " does not exist!");
		}
		
		final Configuration otherConfig = new Configuration(false);
		final FSDataInputStream fsdis = fs.open(defaultConfigFilePath);
		otherConfig.addResource(fsdis);

		for(Entry<String, String> entry : otherConfig) {
			if (overrideValues) {
				conf.set(entry.getKey(), entry.getValue());
			}
			else {
				conf.setIfUnset(entry.getKey(), entry.getValue());
			}
		}
		
		fsdis.close();
	}
}
