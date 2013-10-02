package com._42six.amino.api.job;

import org.apache.hadoop.conf.Configuration;
import com._42six.amino.data.DataLoader;

public interface AminoReuseEnrichmentJob extends AminoEnrichmentJob
{
	
	/**
	 * This allows you to pass a DataLoader that may use a subset of the left side of the join.
	 * Typically, if you are going to reuse previous enrichment results, you would only want to provide the NEW data from this DataLoader for the first phase.
	 * You can extend your existing DataLoader and override the initializeFormat method to only provide a subset.
	 * @return the DataLoader class
	 */
	public Class<? extends DataLoader> getFirstPhaseDataLoaderClass();
	
	
//	/**
//	 * This is where the output from the first phase will be written
//	 * @return the directory in HDFS
//	 */
//	public String getFirstPhaseEnrichmentOutputDirectory();
	
	/**
	 * The job needs to know the output sub directory up front
	 * Note: THIS IS NOT THE FULL PATH, just the sub folder name, the amino.enrichment.output.root from the conf we be the prefix to make up the full path
	 * @return the sub folders in HDFS
	 */
	public String getOutputSubDirectory(Configuration conf);
	
//	/**
//	 * This is the root directory where the output will be written.
//	 * Note it will not be deleted on subsequent runs, the sub folders will be created under it
//	 * @return the root path for all enrichment output
//	 */
//	public Path getEnrichmentRootOutput(Configuration conf);
	
//	/**
//	 * This determines the output sub folder (not the full path) on the fly for the enrichment results.
//	 * @return the sub folder name
//	 */
//	public String getAppropriateOutputSubFolder(MapWritable mw);
	
	/**
	 * This provides you the opportunity to specify which enrichment output sub folders from the first phase to use.
	 * Again, you can do some filtering here.
	 * THIS IS NOT THE FULL PATH, just the sub folder names.  The amino.enrichment.output.root from the conf we be the prefix to make up the full path
	 * @return the directories in HDFS
	 */
	public Iterable<String> getSecondPhaseEnrichmentInputDirectories(Configuration conf);
	
	/**
	 * Gives you the opportunity to do any enrichment directory cleanup after the job runs.
	 */
	public void directoryCleanup(Configuration conf);
}
