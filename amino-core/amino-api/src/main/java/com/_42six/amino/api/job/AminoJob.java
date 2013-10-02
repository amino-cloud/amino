package com._42six.amino.api.job;

import org.apache.hadoop.conf.Configuration;

import com._42six.amino.data.DataLoader;

public interface AminoJob {
	
	public JobOutputEstimate getJobEstimate();
	public String getJobName();

	public Class<? extends DataLoader> getDataLoaderClass(); //This is soups object with the getters it will also provide the hadoop RecordReader and InputFormat needed
	
	public Iterable<Class<? extends AminoReducer>> getAminoReducerClasses();

	/**
	 * If you want to group multiple jobs/data sources into the same domain of Amino, use this ID to set that domain.
	 * If you return an Integer (not null) you must return values for getAminoDomainName and getAminoDomainDescription.
	 * @return the domain id to group this job in, if you don't want to group, return null. 
	 */
	public Integer getAminoDomainID();
	
	/**
	 * The name that describes the AminoDomain (returned from getAminoDomain)
	 * @return the domain name that represents the AminoDomain (returned from getAminoDomain)
	 */
	public String getAminoDomainName();
	
	/**
	 * A description of the AminoDomain (returned from getAminoDomain)
	 * @return the domain description that represents the AminoDomain (returned from getAminoDomain)
	 */
	public String getAminoDomainDescription();
	
	
	/**
	 * Set the configuration for this AminoJob
	 * @param config
	 */
	public void setConfig(Configuration config);
}
