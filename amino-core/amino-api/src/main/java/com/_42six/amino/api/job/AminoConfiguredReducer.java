package com._42six.amino.api.job;

import org.apache.hadoop.conf.Configuration;

public class AminoConfiguredReducer {
	
	private Configuration config;

	public void setConfig(Configuration config) {
		this.config = config;
	}
	
	public Configuration getConfig() {
		return config;
	}
}
