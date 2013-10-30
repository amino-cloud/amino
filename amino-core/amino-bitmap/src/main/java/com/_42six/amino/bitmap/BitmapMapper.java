package com._42six.amino.bitmap;

import com._42six.amino.common.*;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.bucketcache.BucketCache;
import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BitmapMapper extends Mapper<BucketStripped, AminoWritable, BitmapKey, BitmapValue> {

    public static final char SEPARATOR = ':';
    
    private BucketCache bucketCache;
    private int numberOfShards = 10;
    private int numberOfHashes = 1;
    private final FeatureFactTranslatorImpl translator = new FeatureFactTranslatorImpl();
    
    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
    	numberOfShards = context.getConfiguration().getInt(BitmapConfigHelper.BITMAP_CONFIG_NUM_SHARDS, 10);
        numberOfHashes = context.getConfiguration().getInt("amino.bitmap.num-hashes", 1);
    	bucketCache = new BucketCache(context.getConfiguration());
    }

    @Override
    protected void map(BucketStripped bucketStripped, AminoWritable aw, Context context) throws IOException, InterruptedException {
    	final Bucket bucket = bucketCache.getBucket(bucketStripped);
        final Feature feature = aw.getFeature();
        final FeatureFact featureFact = aw.getFeatureFact();
        int featureIndex = BitmapIndex.getFeatureIndex(feature);
        int binNumber = BitmapIndex.getBucketValueIndex(bucketStripped) % numberOfShards;

        final BitmapValue bitmapValue = new BitmapValue();
        final BitmapKey featureKey = new BitmapKey(BitmapKey.KeyType.FEATURE,
                Integer.toString(featureIndex),
                featureFact.toText(translator).toString(), -1, bucket.getBucketVisibility().toString());

        final BitmapKey bucketKey = new BitmapKey(BitmapKey.KeyType.BUCKET,
                String.format("%d%c%s%c%s", binNumber, SEPARATOR, bucket.getBucketDataSource(), SEPARATOR, bucket.getBucketName().toString()),
                bucket.getBucketValue().toString(), -1, bucket.getBucketVisibility().toString());

        for (int i = 0; i < numberOfHashes; i++)
        {
            bitmapValue.setIndex(BitmapIndex.getFeatureFactIndex(bucket, feature, featureFact, i));
            featureKey.setSalt(i);
            bucketKey.setSalt(i);

            context.write(featureKey, bitmapValue);
            context.write(bucketKey, bitmapValue);
        }
    }
}
