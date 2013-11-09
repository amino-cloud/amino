package com._42six.amino.data.impl;

import com._42six.amino.api.framework.AminoDriverUtils;
import com._42six.amino.common.BucketStripped;
import com._42six.amino.common.service.datacache.BucketCache;
import com._42six.amino.data.DataLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class EnrichmentDataLoader implements DataLoader {
	
	@SuppressWarnings("rawtypes")
	private RecordReader reader;
	private String[] dataLocs = null;
	private Configuration conf;
	private BucketCache bucketCache;
	
	private static final String WILL_BE_OVERRIDDEN = "override me";

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() {
		return new SequenceFileInputFormat();
	}

	@Override
	public void initializeFormat(Job job) throws IOException {
		Configuration conf = job.getConfiguration();
		String fullLoc = conf.get(AminoDriverUtils.ENRICHMENT_OUTPUT);
		if (fullLoc.contains(",")) {
			dataLocs = fullLoc.split(",");
		}
		else {
			dataLocs = new String[1];
			dataLocs[0] = fullLoc;
		}
		SequenceFileInputFormat.setInputPaths(job, fullLoc);
		
		bucketCache = new BucketCache(job.getConfiguration());
	}

	@Override
	public boolean canReadFrom(InputSplit inputSplit) {
		FileSplit fs = (FileSplit)inputSplit;
		for (String dataLoc : dataLocs) {
			if (fs.getPath().toString().contains(dataLoc)) {
				System.out.println("canRead=true: " + fs.getPath() + " --- " + dataLoc);
				return true;
			}
			else {
				System.out.println("canRead=true: " + fs.getPath() + " --- " + dataLoc);
			}
		}
		
		return false;
	}

	@Override
	public MapWritable getNext() throws IOException {
		try {
			// We have nothing else so return null
			if (!this.reader.nextKeyValue()) {
				return null;
			}
			if (bucketCache == null) {
				bucketCache = new BucketCache(conf);
			}
			BucketStripped b = (BucketStripped)this.reader.getCurrentKey();
			
			MapWritable mw = (MapWritable)reader.getCurrentValue();
			mw.put(bucketCache.getBucketName(b), b.getBucketValue());
			
			return mw;
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
	
	@Override
	public MapWritable getNextKey(MapWritable currentKey) throws IOException, InterruptedException {
		
		BucketStripped bucket = (BucketStripped)this.reader.getCurrentKey();
		
		if (bucketCache == null) {
			bucketCache = new BucketCache(conf);
		}
		
		MapWritable mw = bucketCache.getBucketAsKey(bucket);
		
		return mw;
	}

	@Override
	public List<Text> getBuckets() {
		//will overriden by getNextKey(MapWritable currentKey)
		return new ArrayList<Text>();
	}

	@Override
	public Hashtable<Text, Text> getBucketDisplayNames() {
		//will overriden by getNextKey(MapWritable currentKey)
		return new Hashtable<Text, Text>();
	}

	@Override
	public String getDataSourceName() {
		//will be overriden by getNextKey
		return WILL_BE_OVERRIDDEN;
	}

	@Override
	public String getDataSetName(MapWritable mw) {
		return mw.get(DataLoader.DATASET_NAME).toString();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void setRecordReader(RecordReader recordReader) throws IOException {
		this.reader = recordReader;
	}

	@Override
	public void setConfig(Configuration config) {
		this.conf = config;
	}

	@Override
	public String getVisibility() {
		//will be overriden by getNextKey
		return WILL_BE_OVERRIDDEN;
	}

	@Override
	public String getHumanReadableVisibility() {
		//will be overriden by getNextKey
		return WILL_BE_OVERRIDDEN;
	}

}

