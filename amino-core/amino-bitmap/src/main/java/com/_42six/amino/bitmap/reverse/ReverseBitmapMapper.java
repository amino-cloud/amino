package com._42six.amino.bitmap.reverse;

import com._42six.amino.bitmap.BitmapConfigHelper;
import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Bucket;
import com._42six.amino.common.BucketStripped;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.datacache.BucketCache;
import com._42six.amino.common.service.datacache.BucketNameCache;
import com._42six.amino.common.service.datacache.DataSourceCache;
import com._42six.amino.common.service.datacache.VisibilityCache;
import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import com._42six.amino.common.translator.FeatureFactTranslatorInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReverseBitmapMapper extends Mapper<BucketStripped, AminoWritable, ReverseBitmapKey, IntWritable>
{
	private BucketCache bucketCache;
    private BucketNameCache bucketNameCache;
    private DataSourceCache dataSourceCache;
    private VisibilityCache visibilityCache;
    private FeatureFactTranslatorInt ffTranslator = new FeatureFactTranslatorImpl();
    private int numberOfShards;
    private int numberOfHashes;

    private BucketStripped previousBS = null;
    private Bucket bucket;
    private int currentShard;
    private int firstIndex; // Could probably cache the other ones too

    private IntWritable bucketNameIndex;
    private IntWritable datasourceIndex;
    private IntWritable visibilityIndex;

    public void setFfTranslator(FeatureFactTranslatorInt ffTranslator) {
        this.ffTranslator = ffTranslator;
    }

    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
        final Configuration configuration = context.getConfiguration();
    	bucketCache = new BucketCache(configuration);
        bucketNameCache = new BucketNameCache(configuration);
        dataSourceCache = new DataSourceCache(configuration);
        visibilityCache = new VisibilityCache(configuration);
		numberOfShards = context.getConfiguration().getInt(BitmapConfigHelper.BITMAP_CONFIG_NUM_SHARDS, 10);
	    numberOfHashes = context.getConfiguration().getInt("amino.bitmap.num-hashes", 1);
    }

	@Override
	protected void map(BucketStripped bs, AminoWritable aw, Context context) throws IOException, InterruptedException
	{
        int index;

        if(previousBS == null || bs.compareTo(previousBS) != 0){
            previousBS = new BucketStripped(bs);
		    bucket = bucketCache.getBucket(bs);
            index = BitmapIndex.getValueIndex(bucket, 0);
            firstIndex = index;
            currentShard = index % numberOfShards;
            bucketNameIndex = bucketNameCache.getIndexForValue(bucket.getBucketName());
            datasourceIndex = dataSourceCache.getIndexForValue(bucket.getBucketDataSource());
            visibilityIndex = visibilityCache.getIndexForValue(bucket.getBucketVisibility());

        } else {
            index = firstIndex;
        }

        final int featureIndex = BitmapIndex.getFeatureIndex(aw.getFeature());

        // Convert the feature fact into a db friendly value
        final String featureValue = aw.getFeatureFact().toText(ffTranslator).toString();

        // Base the shard on the first index.  This way all salted values end up in the same shard.
        final ReverseBitmapKey rbKey = new ReverseBitmapKey(currentShard, 0, datasourceIndex,
                bucketNameIndex, featureIndex, featureValue, visibilityIndex);

        context.write(rbKey, new IntWritable(index));

        // Create the rest of the salt values
        for (int salt = 1; salt < numberOfHashes; salt++)
        {
            rbKey.setSalt(salt);
            context.write(rbKey, new IntWritable(BitmapIndex.getValueIndex(bucket, salt)));
        }
	}
	
}
