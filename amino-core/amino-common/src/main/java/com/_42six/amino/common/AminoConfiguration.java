package com._42six.amino.common;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
	 * @param className
	 * @param overrideValues true to overwrite already-set properties in conf
	 * @throws IOException
	 */
	public static void loadDefault(Configuration conf, String className, boolean overrideValues)
			throws IOException {
		// TODO (soup): Validate parameters and throw exceptions

		final String defaultConfigurationPath = conf
				.get(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY);

		// Make sure that we have a valid Amino Object
		if (defaultConfigurationPath == null) {
			throw new IllegalArgumentException(
					"This configuration does not have the default configuration path for amino specified!");
		}

		FileSystem fs = FileSystem.get(conf);

		final Path defaultConfigFilePath = new Path(String.format("%s/%s.xml",
				defaultConfigurationPath, className));

		if (!fs.exists(defaultConfigFilePath)) {
			throw new IOException("File " + defaultConfigFilePath.toString()
					+ " does not exist!");
		}
		
		Configuration otherConfig = new Configuration(false);
		FSDataInputStream fsdis = fs.open(defaultConfigFilePath);
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
