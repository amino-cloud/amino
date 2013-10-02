package com._42six.amino.bitmap;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Bucket;
import com._42six.amino.common.BucketStripped;
import com._42six.amino.common.Feature;
import com._42six.amino.common.FeatureFact;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.bucketcache.BucketCache;
import com._42six.amino.common.translator.FeatureFactTranslatorImpl;

public class BitmapMapper extends Mapper<BucketStripped, AminoWritable, BitmapKey, BitmapValue> {

    public static final char SEPARATOR = ':';
    
    private BucketCache bucketCache;
    
    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
    	bucketCache = new BucketCache(context.getConfiguration());
    }
    
    @Override
    protected void map(BucketStripped bucketStripped, AminoWritable aw, Context context) throws IOException, InterruptedException {

    	Bucket bucket = bucketCache.getBucket(bucketStripped);
    	
    	final int numberOfShards = context.getConfiguration().getInt(BitmapConfigHelper.BITMAP_CONFIG_NUM_SHARDS, 10);
        final int numberOfHashes = context.getConfiguration().getInt("amino.bitmap.num-hashes", 1);

        Feature feature = aw.getFeature();
        FeatureFact featureFact = aw.getFeatureFact();
        int featureIndex = BitmapIndex.getFeatureIndex(feature);
        int binNumber = BitmapIndex.getBucketValueIndex(bucket) % numberOfShards;

        for (int i = 0; i < numberOfHashes; i++) 
        {
        	BitmapKey featureKey = new BitmapKey(BitmapKey.KeyType.FEATURE,
                    Integer.toString(featureIndex),
                    featureFact.toText(new FeatureFactTranslatorImpl()).toString(), i, bucket.getBucketVisibility().toString());
            BitmapValue featureValue = new BitmapValue();

            
        	BitmapKey bucketKey = new BitmapKey(BitmapKey.KeyType.BUCKET,
                    String.format("%d%c%s%c%s", binNumber, SEPARATOR, bucket.getBucketDataSource(), SEPARATOR, bucket.getBucketName().toString()),
                    bucket.getBucketValue().toString(), i, bucket.getBucketVisibility().toString());
            BitmapValue bucketValue = new BitmapValue();
            
            int featureFactIndex = BitmapIndex.getFeatureFactIndex(bucket, feature, featureFact, i);
            featureValue.addIndex(featureFactIndex);
            bucketValue.addIndex(featureFactIndex);
            
            context.write(featureKey, featureValue);
            context.write(bucketKey, bucketValue);
        }
    }
}
