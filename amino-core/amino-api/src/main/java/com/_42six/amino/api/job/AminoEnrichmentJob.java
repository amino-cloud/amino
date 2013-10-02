package com._42six.amino.api.job;


import com._42six.amino.data.DataLoader;
import com._42six.amino.data.EnrichWorker;

public interface AminoEnrichmentJob extends AminoJob
{
	/**
	 * The DataLoader class that represents your enrichment source
	 * @return the DataLoader class
	 */
	//public Class<? extends DataLoader> getEnrichmentDataLoader();
	public Iterable<Class<? extends DataLoader>> getEnrichmentDataLoaders();
	
	/**
	 * The EnrichWorker class that manages the join of the datasources
	 * @return the EnrichWorker class
	 */
	public Class<? extends EnrichWorker> getEnrichWorker();

}
