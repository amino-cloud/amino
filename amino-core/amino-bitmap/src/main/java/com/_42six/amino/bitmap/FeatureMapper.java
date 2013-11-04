package com._42six.amino.bitmap;

import com._42six.amino.common.*;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.bucketcache.BucketCache;
import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FeatureMapper extends Mapper<BucketStripped, AminoWritable, FeatureKey, BitmapValue> {

    private BucketCache bucketCache;
    private int numberOfHashes = 1;
    private final FeatureFactTranslatorImpl translator = new FeatureFactTranslatorImpl();
    
    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
        numberOfHashes = context.getConfiguration().getInt("amino.bitmap.num-hashes", 1);
    	bucketCache = new BucketCache(context.getConfiguration());
    }

    @Override
    protected void map(BucketStripped bucketStripped, AminoWritable aw, Context context) throws IOException, InterruptedException {
    	final Bucket bucket = bucketCache.getBucket(bucketStripped);
        final Feature feature = aw.getFeature();
        final FeatureFact featureFact = aw.getFeatureFact();
        final int featureIndex = BitmapIndex.getFeatureIndex(feature);

        final BitmapValue bitmapValue = new BitmapValue();
        final FeatureKey featureKey = new FeatureKey(featureIndex, featureFact.toText(translator).toString(), bucket.getBucketVisibility().toString());

        for (int i = 0; i < numberOfHashes; i++)
        {
            bitmapValue.setIndex(BitmapIndex.getFeatureFactIndex(bucket, feature, featureFact, i));
            featureKey.setSalt(i);

            context.write(featureKey, bitmapValue);
        }
    }
}
