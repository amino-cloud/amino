package com._42six.amino.bitmap.reverse;

import com._42six.amino.bitmap.BitmapConfigHelper;
import com._42six.amino.common.*;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.bucketcache.BucketCache;
import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import com._42six.amino.common.translator.FeatureFactTranslatorInt;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//public class ReverseBitmapMapper extends Mapper<BucketStripped, AminoWritable, ReverseBitmapKey, ReverseBitmapValue>
public class ReverseBitmapMapper extends Mapper<BucketStripped, AminoWritable, ReverseBitmapKey, IntWritable>
{
	private BucketCache bucketCache;
    private FeatureFactTranslatorInt ffTranslator = new FeatureFactTranslatorImpl();

    public void setFfTranslator(FeatureFactTranslatorInt ffTranslator) {
        this.ffTranslator = ffTranslator;
    }

    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
    	bucketCache = new BucketCache(context.getConfiguration());
    }

	@Override
	protected void map(BucketStripped bs, AminoWritable aw, Context context) throws IOException, InterruptedException 
	{
		final int numberOfShards = context.getConfiguration().getInt(BitmapConfigHelper.BITMAP_CONFIG_NUM_SHARDS, 10);
	    final int numberOfHashes = context.getConfiguration().getInt("amino.bitmap.num-hashes", 1);
		final Bucket bucket = bucketCache.getBucket(bs);
	    final Feature feature = aw.getFeature();
        final FeatureFact featureFact = aw.getFeatureFact();
        final int featureIndex = BitmapIndex.getFeatureIndex(feature);

        // Convert the feature fact into a db friendly value
        final String featureValue = featureFact.toText(ffTranslator).toString();

        // Base the shard on the first index.  This was all salted values end up in the same shard.
        int index = BitmapIndex.getValueIndex(bucket, 0);
        final int shard = index % numberOfShards;
        final ReverseBitmapKey rbKey = new ReverseBitmapKey(shard, 0, bucket.getBucketDataSource().toString(),
                bucket.getBucketName().toString(), featureIndex, featureValue, bucket.getBucketVisibility().toString());
        //final ReverseBitmapValue rbValue = new ReverseBitmapValue(index, bucket.getBucketName().toString(), bucket.getBucketValue().toString());
        //context.write(rbKey, rbValue);
        if(bucket.getBucketValue().toString().equals("6")){
            System.out.println(rbKey.toString() + " | " + index);
        }
        context.write(rbKey, new IntWritable(index));

        // Create the rest of the salt values
        for (int salt = 1; salt < numberOfHashes; salt++)
        {
            index = BitmapIndex.getValueIndex(bucket, salt);
            rbKey.setSalt(salt);
            //rbValue.setBucketValueIndex(index);
            //context.write(rbKey, rbValue);
            if(bucket.getBucketValue().toString().equals("6")){
                System.out.println(rbKey.toString() + " | " + index);
            }
            context.write(rbKey, new IntWritable(index));
        }
	}
	
}
