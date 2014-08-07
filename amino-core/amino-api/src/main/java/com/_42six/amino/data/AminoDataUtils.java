package com._42six.amino.data;

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;

public class AminoDataUtils {
	private final static String AMINO_DATA_LOADER_CLASS_KEY = "amino.dataloader";
	private final static String AMINO_JOIN_DATA_LOADER_CLASS_KEY = "amino.join.dataloader";
	private final static String AMINO_ENRICH_WORKER_CLASS_KEY = "amino.enrichment.worker";

    /**
     * Creates a {@link com._42six.amino.data.DataLoader} referenced in the config and calls it's setConfig() method
     *
     * @param config The {@link org.apache.hadoop.conf.Configuration } containing the DataLoader to load and values to
     *               intialize it with
     * @return The initialized {@link com._42six.amino.data.DataLoader}
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
	public static DataLoader createDataLoader(Configuration config) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		final Class<? extends DataLoader> loaderClass = config.getClass(AMINO_DATA_LOADER_CLASS_KEY, null, DataLoader.class);
		if(loaderClass == null) {
			throw new ClassNotFoundException("Could not find data loader class in config key '" +AMINO_DATA_LOADER_CLASS_KEY + "'");
		} 
		DataLoader retVal = loaderClass.newInstance();
		retVal.setConfig(config);
		return retVal;
	}
	
	public static void setDataLoader(Configuration config, DataLoader loader) {
		config.setClass(AMINO_DATA_LOADER_CLASS_KEY, loader.getClass(), DataLoader.class);
	}
	
	public static Iterable<DataLoader> getJoinDataLoaders(Configuration config) throws ClassNotFoundException, InstantiationException, IllegalAccessException
	{
		final ArrayList<DataLoader> loaders = new ArrayList<>();
		for (int i = 0; i < Integer.MAX_VALUE; i++)
		{
			final Class<? extends DataLoader> loaderClass = config.getClass(AMINO_JOIN_DATA_LOADER_CLASS_KEY + ":" + i, null, DataLoader.class);
			if(loaderClass == null) {
                if (i == 0) {
                    throw new ClassNotFoundException("Could not find JOIN dataloader class in config key '"+AMINO_ENRICH_WORKER_CLASS_KEY+":0'");
                } else {
                    break;
                }
            }

			DataLoader retVal = loaderClass.newInstance();
			retVal.setConfig(config);
			loaders.add(retVal);
		}
		return loaders;
	}

	public static void setJoinDataLoaders(Configuration config, Iterable<Class<? extends DataLoader>> loaders) throws InstantiationException, IllegalAccessException {
		int counter = 0;
		for (Class<? extends DataLoader> dlClass : loaders)
		{
			final DataLoader dl = dlClass.newInstance();
			config.setClass(AMINO_JOIN_DATA_LOADER_CLASS_KEY + ":" + Integer.toString(counter), dl.getClass(), DataLoader.class);
			counter++;
		}
	}
	
	public static EnrichWorker getEnrichWorker(Configuration config) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		Class<? extends EnrichWorker> worker = config.getClass(AMINO_ENRICH_WORKER_CLASS_KEY, null, EnrichWorker.class);
		if(worker == null) {
			throw new ClassNotFoundException("Could not find enrichment worker class in config key '"+AMINO_ENRICH_WORKER_CLASS_KEY+"'");
		} 
		return worker.newInstance();
	}
	
	public static void setEnrichWorker(Configuration config, EnrichWorker ew) {
		config.setClass(AMINO_ENRICH_WORKER_CLASS_KEY, ew.getClass(), EnrichWorker.class);
	}

}
