package com._42six.amino.bitmap;

import com._42six.amino.common.*;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.datacache.BucketCache;
import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StatsMapper extends Mapper<BucketStripped, AminoWritable, StatsKey, Text> 
{
	
	private BucketCache bucketCache;
    
    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
    	bucketCache = new BucketCache(context.getConfiguration());
    }

	@Override
    protected void map(BucketStripped bucketStripped, AminoWritable aw, Context context) throws IOException, InterruptedException 
    {
//		final int numberOfHashes = context.getConfiguration().getInt("amino.bitmap.num-hashes", 1);
		
		Bucket bucket = bucketCache.getBucket(bucketStripped);
		
		Feature feature = aw.getFeature();
        FeatureFact featureFact = aw.getFeatureFact();
        int featureIndex = BitmapIndex.getFeatureIndex(feature);

		StatsKey featureKey = new StatsKey(Integer.toString(featureIndex),
				featureFact.toText(new FeatureFactTranslatorImpl()).toString(), bucket.getBucketName().toString(), bucket.getBucketVisibility().toString());
		
//		for (int i = 0; i < numberOfHashes; i++) 
//        {
//			int featureFactIndex = BitmapIndex.getFeatureFactIndex(bucket, feature, featureFact, i);
//			featureKey.saltedIndexes.put(i, featureFactIndex);
//        }
		
		context.write(featureKey, bucket.getBucketValue());
		
	}
}
