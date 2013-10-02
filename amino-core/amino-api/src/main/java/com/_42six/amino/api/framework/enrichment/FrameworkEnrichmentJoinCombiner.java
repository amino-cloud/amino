package com._42six.amino.api.framework.enrichment;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com._42six.amino.api.framework.AminoDriverUtils;
import com._42six.amino.common.BucketStripped;

public class FrameworkEnrichmentJoinCombiner extends Reducer<EnrichmentJoinKey, MapWritable, EnrichmentJoinKey, MapWritable> {
	
	@Override
	public void reduce(EnrichmentJoinKey key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		
		ArrayList<BucketStripped> written = new ArrayList<BucketStripped>();
		for (MapWritable value : values) {
			Writable ok = value.get(new Text(AminoDriverUtils.AMINO_ENRICHMENT_BUCKET));
			if (ok != null) {
				//This will avoid passing the same bucket more than once
				//Remember, to get this far, these are all the buckets that have the same enrichment key at some point - and could be more than once
				BucketStripped bucket = (BucketStripped)ok;
				if (!written.contains(bucket)) {
					context.write(key, value);
					written.add(bucket);
				}
			}
			else{
				context.write(key, value);
			}
		}
	}
}
