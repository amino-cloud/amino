package com._42six.amino.api.framework.enrichment;

import com._42six.amino.api.framework.AminoDriverUtils;
import com._42six.amino.common.BucketStripped;
import com._42six.amino.data.DataLoader;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class FrameworkEnrichmentJoinReducer extends Reducer<EnrichmentJoinKey, MapWritable, BucketStripped, MapWritable> {
	
	private BucketStripped currentBucket = null;
	private static final Text datasetKey = new Text(DataLoader.DATASET_NAME);
	private static final Text enrichBucketKey = new Text(AminoDriverUtils.AMINO_ENRICHMENT_BUCKET);
	
	private long maxPerDataset;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);	
		maxPerDataset = context.getConfiguration().getLong("enrich.records.per.key.per.dataset.max.global", Long.MAX_VALUE);
	}

	@Override
	public void reduce(EnrichmentJoinKey key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		Map<Writable, HashSet<MapWritable>> enrichMap = new HashMap<>();
		BucketStripped bucket;

		for (MapWritable value : values) {
			Writable enrichBucket = value.get(enrichBucketKey);

			//We should get all these enrichment values first (due to the custom comparator classes), need to hold them in memory
			if (enrichBucket == null) {
				Writable dataset = value.get(datasetKey);
				if (!enrichMap.containsKey(dataset)) {
					HashSet<MapWritable> enrichSet = new HashSet<>();
					enrichSet.add(new MapWritable(value));
					enrichMap.put(dataset,  enrichSet);
				}
				//Keep adding to this dataset's enrich records until we hit the max per dataset
				else if (enrichMap.get(dataset).size() < maxPerDataset) {
					//Had to use this constructor - spent hours trying to find this bug
					enrichMap.get(dataset).add(new MapWritable(value));
				}
			}
			else {
				bucket = (BucketStripped)enrichBucket;
				//Fix - no need to write the subject out, just write the enrich values with the proper buckets, then, use the enrich DataLoader
				//The follow on job will use both DataLoaders
				//context.write(bucket, value);

				//Again, the custom comparator does a secondary sort on the bucket, so we should get these in order after all the enrich values
				if (currentBucket == null || !currentBucket.equals(bucket)) {
					for (HashSet<MapWritable> enrichSet : enrichMap.values()) {
						for (MapWritable val : enrichSet) {
							MapWritable newVal = new MapWritable(val);
							context.write(bucket, newVal);
						}
					}
				}
				currentBucket = bucket;
			}
		}

		//I don't need this...there was never a bug - the way it works is the enrichment values are written as soon as a new Bucket appears
		//Write out the last set
//		if (bucket != null)
//		{
//			for (HashSet<MapWritable> enrichSet : enrichMap.values()) {
//				for (MapWritable val : enrichSet)
//				{
//					MapWritable newVal = new MapWritable(val);
//					context.write(bucket, newVal);
//				}
//			}
//		}
	}
}
