package com._42six.amino.data;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

public class AminoDataUtils {
	private final static String AMINO_DATA_LOADER_CLASS_KEY = "amino.dataloader";
	private final static String AMINO_JOIN_DATA_LOADER_CLASS_KEY = "amino.join.dataloader";
	private final static String AMINO_ENRICH_WORKER_CLASS_KEY = "amino.enrichment.worker";
	
	public static DataLoader getDataLoader(Configuration config) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		Class<? extends DataLoader> tmp = config.getClass(AMINO_DATA_LOADER_CLASS_KEY, null, DataLoader.class);
		if(tmp == null) {
			throw new ClassNotFoundException("Could not find data loader class in configuraiton object.  Please set the data loader!");
		} 
		DataLoader retVal = tmp.newInstance();
		retVal.setConfig(config);
		return retVal;
	}
	
	public static void setDataLoader(Configuration config, DataLoader loader) {
		config.setClass(AMINO_DATA_LOADER_CLASS_KEY, loader.getClass(), DataLoader.class);
	}
	
	//public static DataLoader getJoinDataLoader(Configuration config) throws ClassNotFoundException, InstantiationException, IllegalAccessException 
	public static Iterable<DataLoader> getJoinDataLoaders(Configuration config) throws ClassNotFoundException, InstantiationException, IllegalAccessException 
	{
		ArrayList<DataLoader> loaders = new ArrayList<DataLoader>();
		for (int i = 0; i < Integer.MAX_VALUE; i++)
		{
			Class<? extends DataLoader> tmp = config.getClass(AMINO_JOIN_DATA_LOADER_CLASS_KEY + ":" + i, null, DataLoader.class);
			if(tmp == null && i == 0) {
				throw new ClassNotFoundException("Could not find RIGHT data loader class in configuraiton object.  Please set the data loader!");
			} 
			else if (tmp == null) break;
			DataLoader retVal = tmp.newInstance();
			retVal.setConfig(config);
			loaders.add(retVal);
			//return retVal;
		}
		return loaders;
	}
	
	//public static void setJoinLoader(Configuration config, DataLoader loader) {
	public static void setJoinDataLoaders(Configuration config, Iterable<Class<? extends DataLoader>> loaders) throws InstantiationException, IllegalAccessException {
		//config.setClass(AMINO_JOIN_DATA_LOADER_CLASS_KEY, loader.getClass(), DataLoader.class);
		int counter = 0;
		for (Class<? extends DataLoader> dlClass : loaders)
		{
			DataLoader dl = dlClass.newInstance();
			config.setClass(AMINO_JOIN_DATA_LOADER_CLASS_KEY + ":" + Integer.toString(counter), dl.getClass(), DataLoader.class);
			counter++;
		}
	}
	
	public static EnrichWorker getEnrichWorker(Configuration config) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		Class<? extends EnrichWorker> tmp = config.getClass(AMINO_ENRICH_WORKER_CLASS_KEY, null, EnrichWorker.class);
		if(tmp == null) {
			throw new ClassNotFoundException("Could not find the enrichment worker class in configuraiton object.  Please set the data loader!");
		} 
		EnrichWorker retVal = tmp.newInstance();
		return retVal;
	}
	
	public static void setEnrichWorker(Configuration config, EnrichWorker ew) {
		config.setClass(AMINO_ENRICH_WORKER_CLASS_KEY, ew.getClass(), EnrichWorker.class);
	}

}
