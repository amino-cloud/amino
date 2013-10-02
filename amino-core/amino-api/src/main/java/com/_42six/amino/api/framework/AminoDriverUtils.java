package com._42six.amino.api.framework;

import org.apache.hadoop.conf.Configuration;

import com._42six.amino.api.job.AminoJob;
import com._42six.amino.api.job.AminoReducer;

public class AminoDriverUtils 
{
	private static final String AMINO_REDUCER_CLASS_KEY = "amino.reducer.class";
	private static final String AMINO_JOB_CLASS_KEY = "amino.job.class";
	public static final String AMINO_ENRICHMENT_BUCKET = "amino.enrichment.bucket";
	public static final String ENRICHMENT_ROOT_OUTPUT = "amino.enrichment.output.root";
	public static final String ENRICHMENT_OUTPUT = "amino.enrichment.output";
	
	public static AminoReducer getAminoReducer(Configuration config) throws ClassNotFoundException, InstantiationException, IllegalAccessException 
	{
		Class<? extends AminoReducer> tmp = config.getClass(AMINO_REDUCER_CLASS_KEY, null, AminoReducer.class);
		if(tmp == null) {
			throw new ClassNotFoundException("Could not find amino reducer class in configuraiton object");
		} 
		AminoReducer retVal = tmp.newInstance();
		retVal.setConfig(config);
		return retVal;
	}
	
	public static void setAminoReducer(Configuration config, Class<? extends AminoReducer> ar) 
	{
		config.setClass(AMINO_REDUCER_CLASS_KEY, ar, AminoReducer.class);
	}
	

	
	public static AminoJob getAminoJob(Configuration config) throws ClassNotFoundException, InstantiationException, IllegalAccessException 
	{
		Class<? extends AminoJob> tmp = config.getClass(AMINO_JOB_CLASS_KEY, null, AminoJob.class);
		if(tmp == null) {
			throw new ClassNotFoundException("Could not find amino job class in configuraiton object.");
		} 
		AminoJob retVal = tmp.newInstance();
		retVal.setConfig(config);
		return retVal;
	}
	
	public static void setAminoJob(Configuration config, Class<? extends AminoJob> aj) 
	{
		config.setClass(AMINO_JOB_CLASS_KEY, aj, AminoJob.class);
	}
}
