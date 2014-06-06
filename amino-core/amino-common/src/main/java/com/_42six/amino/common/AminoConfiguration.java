package com._42six.amino.common;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map.Entry;

public class AminoConfiguration extends Configuration {

	public static final String DEFAULT_CONFIGURATION_PATH_KEY = "amino.default.configuration.path";

	/**
	 * Loads properties from file and merges into conf.
	 * TODO: Note that this allows anyone running a job to overwrite already-set
	 * config files in hadoop.  When we eventually have different devs using 
	 * amino on the same machine to run different jobs, we should ensure that they
	 * can't overwrite any hadoop property.
	 * 
	 * @param conf current properties
	 * @param className The name of the class to look for the config for.  This is will look for a configuration file
     *                  with the same name as the class
	 * @param overrideValues true to overwrite already-set properties in conf
	 * @throws IOException
	 */
	public static void loadDefault(Configuration conf, String className, boolean overrideValues)
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
