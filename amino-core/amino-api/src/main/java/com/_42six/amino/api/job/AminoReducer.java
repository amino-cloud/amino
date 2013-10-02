package com._42six.amino.api.job;

import org.apache.hadoop.conf.Configuration;

import com._42six.amino.api.model.DatasetCollection;
import com._42six.amino.common.AminoWritable;

public interface AminoReducer 
{
	public Iterable<AminoWritable> reduce(DatasetCollection datasets);
	
	/**
	 * Set the configuration for this AminoReducer
	 * @param config
	 */
	public void setConfig(Configuration config);
}
