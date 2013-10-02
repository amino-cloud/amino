package com._42six.amino.data;

import org.apache.hadoop.io.MapWritable;

public interface EnrichWorker 
{
//	/**
//	 * Determine which DataLoader this source this line belongs to.  
//	 * The "row" parameter is the result of getCurrentValue called against the RecordReader
//	 * @return the DataLoader that matches
//	 */
//	public Class<? extends DataLoader> determineDataLoaderFromRawLine(Object row);

	/**
	 * What the left-side join should use (the subject datasource)
	 * @return left-side join key
	 */
	public Iterable<String> getSubjectKey(MapWritable mw);

	/**
	 * What the right-side join should use (the enrichment datasource)
	 * @return right-side join key
	 */
	public Iterable<String> getEnrichmentKey(MapWritable mw);
	
}
