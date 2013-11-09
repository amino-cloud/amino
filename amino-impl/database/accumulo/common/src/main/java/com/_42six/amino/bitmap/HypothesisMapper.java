package com._42six.amino.bitmap;

import com._42six.amino.common.*;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.datacache.BucketCache;
import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class HypothesisMapper extends Mapper<BucketStripped, AminoWritable, Text, StatsKey>
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
		Bucket bucket = bucketCache.getBucket(bucketStripped);
		
		final int numberOfHashes = context.getConfiguration().getInt("amino.bitmap.num-hashes", 1);
		
		Feature feature = aw.getFeature();
        FeatureFact featureFact = aw.getFeatureFact();
        int featureIndex = BitmapIndex.getFeatureIndex(feature);

		final Text bucketName = bucket.getBucketName();
		final Text bucketVis  = bucket.getBucketVisibility();
		final Text row = new Text();
		final Text cf = new Text();

		for (int i = 0; i < numberOfHashes; i++) 
        {
			int featureFactIndex = BitmapIndex.getFeatureFactIndex(bucket, feature, featureFact, i);

			StatsKey featureKey = new StatsKey(Integer.toString(featureIndex),
					featureFact.toText(new FeatureFactTranslatorImpl()).toString(), bucketName.toString(), bucketVis.toString());
			featureKey.salt = i;
			featureKey.bitmapIndex = featureFactIndex;

	        row.set(Integer.toString(featureFactIndex));
	        cf.set(Integer.toString(i) + "#" + bucketName);
	        Key btKey = new Key(row, cf, new Text(Integer.toString(featureIndex)), bucketVis);
	        //Key btKey = new Key(row, cf, bucketName, bucketVis);
			//Key btKey = new Key(new Text(Integer.toString(featureFactIndex)), new Text(Integer.toString(i)), new Text(bucket.getBucketName().toString()), new Text(bucket.getBucketVisibility().toString()));

			//context.write(new Text(Integer.toString(featureFactIndex)), featureKey);
			context.write(new Text(btKey.toStringNoTime()), featureKey);
        }
	}

}
