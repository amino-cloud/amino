package com._42six.amino.data;

import java.io.IOException;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 *	This interface describes a data loader or a class which will load the data
 *  for a Amino reducer to operate off of.  The major concepts of this interface are 
 *  borrowed from Apache PIG.   
 */
public interface DataLoader {
	
	/**
	 * Don't touch this...will get set with the getDataSetName(MapWritable mw)
	 */
	public static final Text DATASET_NAME = new Text("~setn");
	
	/**
	 * Get the input format that is associated with this loader.
	 * @return the input format associated with this loader
	 */
	@SuppressWarnings("rawtypes")
	public InputFormat getInputFormat();
	
	/**
	 * Intitialize the data format
	 * @param job the hadoop job that needs to be passed to the data loader
	 */
	public void initializeFormat(Job job) throws IOException;
	
	/**
	 * Determine if this DataLoader can read from the provided InputSplit.  This is used on enrichment jobs, if not implemented correctly, enrichment jobs that use this DataLoader will break.
	 * If reading from HDFS, it is recommended that you cast the InputSplit to a FileSplit and call getPath() to see if it matches the source file location.
	 * Unless both dataloaders are reading from the same HDFS directory, then you should examine the rawRowOfData to see if it's the expected data
	 * If reading from Accumulo you can examine the rawRowOfData to see if it's the expected data
	 * @param inputSplit the input split for the data
	 * @param rawRowOfData a row of data randomly read from the input split
	 * @return true if this is the data which the DataLoader is intended to read
	 */
	public boolean canReadFrom(InputSplit inputSplit);
	
	/**
	 * Get the next MapWritable from the data object
	 * @return a map writable containing the keys and values or null if there is nothing more to read
	 * @throws IOException
	 */
	public MapWritable getNext() throws IOException;
	
	/**
	 * Typically you would just return the currentKey back, unless you have dynamic keys based on the content of the value
	 * The key contains the buckets and visibilities, look at AminoRecordReader to understand the currentKey further.
	 * @param currentKey this is the existing key, typically this is reused and doesn't change
	 * @return a map writable containing the key
	 * @throws IOException
	 */
	public MapWritable getNextKey(MapWritable currentKey) throws IOException, InterruptedException;
	
	/**
	 * Get the buckets attached with this data type
	 * @return buckets for this data type
	 */
	public List<Text> getBuckets();
	
	/**
	 * Get the bucket display attached with this data type
	 * @return bucket display names for this data type
	 */
	public Hashtable<Text,Text> getBucketDisplayNames();
	
	/**
	 * Get the name of the data source
	 * @return the name of the data source
	 */
	public String getDataSourceName();
	
	/**
	 * Get the name of the data set based on the contents of the MapWritable (you could have more than one type of dataset in the datasource)
	 * @return the name of the data set, typically just an explicit string, but if there is more than one dataset in the datasource, you may want to 
	 * examine the MapWritable to determine the dataset name.
	 */
	public String getDataSetName(MapWritable mw);
	
	/**
	 * Set the record reader that we are going to use
	 * @param recordReader the record reader that is going to be use
	 */
	@SuppressWarnings("rawtypes")
	public void setRecordReader(RecordReader recordReader) throws IOException;
	
	/**
	 * Set the configuration for this DataLoader
	 * @param config
	 */
	public void setConfig(Configuration config);
	
	/**
	 * Visibility assigned to this datasource.  This will be in the Accumulo visibility cell.
	 */
	public String getVisibility();
	
	/**
	 * Visibility assigned to this datasource.  This will be a human readable string which represents the getVisibility() value, e.g. the Accumulo visibility string.
	 */
	public String getHumanReadableVisibility();
}
