package com._42six.amino.api.framework;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com._42six.amino.common.Bucket;
import com._42six.amino.common.BucketStripped;
import com._42six.amino.common.service.bucketcache.BucketCache;

public final class FrameworkMapper extends Mapper<MapWritable, MapWritable, BucketStripped, MapWritable> {
	
	public void map(MapWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
		
		// Go through available buckets. If this record has a value for that bucket, write the record to the context
		for (Bucket bucket : BucketCache.getBuckets(key)) {
			if (value.containsKey(bucket.getBucketName())) {
				Text bucketValue = (Text)value.get(bucket.getBucketName());
				Bucket bucketCopy = new Bucket(bucket);
				bucketCopy.setBucketValue(bucketValue);
				bucketCopy.computeHash();
				
				//Convert the full bucket to a light weight bucket for hdfs
				BucketStripped stripped = BucketStripped.fromBucket(bucketCopy);
				context.write(stripped, value);
			}
		}
	}
	
	//for debug
	@SuppressWarnings("unused")
	private String mapToString(MapWritable mw) {
		StringBuilder builder = new StringBuilder();
		for (Writable w : mw.keySet()) {
			builder.append("[");
			builder.append(w);
			builder.append("=");
			builder.append(mw.get(w));
			builder.append("]");
		}
		return builder.toString();
	}
}
