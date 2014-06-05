package com._42six.amino.common.service.datacache;

import com._42six.amino.common.Bucket;
import com._42six.amino.common.BucketStripped;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.util.PathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.*;

public class BucketCache {
	
	private Map<IntWritable, Bucket> bucketCache;
	public static final String BUCKET_CACHE_FOLDER = "/buckets";
	private static final Text MAPWRITABLE_BUCKET_KEY = new Text("bk");
	
	public BucketCache() {
		bucketCache = new HashMap<IntWritable, Bucket>();
	}
	
	public BucketCache(Configuration conf) throws IOException {
		bucketCache = new HashMap<IntWritable, Bucket>();
		
		for (String cachePath : PathUtils.getCachePaths(conf)) {
			String bucketCachePath = cachePath + BUCKET_CACHE_FOLDER;
            MapFile.Reader reader = new MapFile.Reader(new Path(bucketCachePath), conf);
			IntWritable key = new IntWritable();
			Bucket value = new Bucket();
			while (reader.next(key, value)) {
				bucketCache.put(new IntWritable(key.get()), new Bucket(value));
			}
		}
		
		for (IntWritable i : bucketCache.keySet()) {
			System.out.println("Loaded bucket from cache:" + i.get() + ":" + bucketCache.get(i));
		}
		
	}
	
	public void addBucket(Bucket bucket) {
		int index = BitmapIndex.getBucketCacheIndex(bucket);
		bucketCache.put(new IntWritable(index), bucket);
	}
	
	@SuppressWarnings("unchecked")
	public void writeToDisk(Configuration conf, boolean writeToDistributedCache) throws IOException {
		String bucketCachePath = PathUtils.getCachePath(conf) + BUCKET_CACHE_FOLDER;

		FileSystem fs = FileSystem.get(conf);
		MapFile.Writer writer = null; 
		
		try {
			writer = new MapFile.Writer(conf, new Path(bucketCachePath), MapFile.Writer.keyClass(IntWritable.class),
                    MapFile.Writer.valueClass(Bucket.class));
			
			ArrayList<IntWritable> keyList = new ArrayList<IntWritable>();
			for (IntWritable i : bucketCache.keySet()) {
				keyList.add(i);
			}

			Collections.sort(keyList);
			for (IntWritable i : keyList) {
				writer.append(i, bucketCache.get(i));
			}
		}
		finally {
			if (writer != null) {
				IOUtils.closeStream(writer);
			}
		}
		
		if (writeToDistributedCache) {
			for (FileStatus status : fs.listStatus(new Path(bucketCachePath))) {
				if (!status.isDirectory()) {
					DistributedCache.addCacheFile(status.getPath().toUri(), conf);
				}
			}
		}
	}
	
	public Bucket getBucket(BucketStripped bucketStripped) throws IOException {
		Bucket bucket = new Bucket(bucketCache.get(bucketStripped.getCacheHash()));
		bucket.setBucketValue(new Text(bucketStripped.getBucketValue()));
		bucket.computeHash();
		return bucket;
	}
	
	public Text getBucketName(BucketStripped bucketStripped) {
		Bucket bucket = bucketCache.get(bucketStripped.getCacheHash());
		return bucket.getBucketName();
	}

	public MapWritable toMapWritableKey() {
		MapWritable mw = new MapWritable();
		MapWritable bucketMap = new MapWritable();

		for (IntWritable key : bucketCache.keySet()) {
			bucketMap.put(key, bucketCache.get(key));
		}
		mw.put(MAPWRITABLE_BUCKET_KEY, bucketMap);		
		
		return mw;
	}
	
	public MapWritable getBucketAsKey(BucketStripped bucketStripped) throws IOException {
		MapWritable mw = new MapWritable();
		MapWritable bucketMap = new MapWritable();
		
		Bucket bucket = getBucket(bucketStripped);
		bucketMap.put(new IntWritable(bucket.hashCode()), bucket);
		mw.put(MAPWRITABLE_BUCKET_KEY, bucketMap);
		return mw;
	}
	
	public static Collection<Bucket> getBuckets(MapWritable key) {
		Collection<Bucket> bucketList = new ArrayList<Bucket>();
		MapWritable bucketMap = (MapWritable)key.get(MAPWRITABLE_BUCKET_KEY);
		for (Writable w : bucketMap.values()) {
			bucketList.add((Bucket)w);
		}
		return bucketList;
	}

}
