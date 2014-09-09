package com._42six.amino.bitmap.reverse;

import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Bucket;
import com._42six.amino.common.BucketStripped;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.datacache.BucketCache;
import com._42six.amino.common.service.datacache.SortedIndexCache;
import com._42six.amino.common.service.datacache.SortedIndexCacheFactory;
import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import com._42six.amino.common.translator.FeatureFactTranslatorInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReverseBitmapMapper extends Mapper<BucketStripped, AminoWritable, ReverseBitmapKey, IntWritable>
{
	private BucketCache bucketCache;
    private SortedIndexCache dataSourceCache;
    private FeatureFactTranslatorInt ffTranslator = new FeatureFactTranslatorImpl();
    private int numberOfShards;
    private int numberOfHashes;

    private BucketStripped previousBS = null;
    private Bucket bucket;
    private int currentShard;

    private VIntWritable datasourceIndex;

    public void setFfTranslator(FeatureFactTranslatorInt ffTranslator) {
        this.ffTranslator = ffTranslator;
    }

    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
        final Configuration configuration = context.getConfiguration();
    	bucketCache = new BucketCache(configuration);
        dataSourceCache = SortedIndexCacheFactory.getCache(SortedIndexCacheFactory.CacheTypes.Datasource, configuration);
		numberOfShards = context.getConfiguration().getInt(AminoConfiguration.NUM_SHARDS, 10);
	    numberOfHashes = context.getConfiguration().getInt(AminoConfiguration.NUM_HASHES, 1);
    }

	@Override
	protected void map(BucketStripped bs, AminoWritable aw, Context context) throws IOException, InterruptedException
	{
        if(previousBS == null || bs.compareTo(previousBS) != 0){
            previousBS = new BucketStripped(bs);
		    bucket = bucketCache.getBucket(bs);
            currentShard = BitmapIndex.getValueIndex(bucket, 0) % numberOfShards;
            datasourceIndex = dataSourceCache.getIndexForValue(bucket.getBucketDataSource());
        }

        final int featureIndex = BitmapIndex.getFeatureIndex(aw.getFeature());

        // Convert the feature fact into a db friendly value
        final String featureValue = aw.getFeatureFact().toText(ffTranslator).toString();

        // Base the shard on the first index.  This way all salted values end up in the same shard.
        final ReverseBitmapKey rbKey = new ReverseBitmapKey(currentShard, 0, datasourceIndex,
                bucket.getBucketName(), featureIndex, featureValue, bucket.getBucketVisibility());

        // Create the rest of the salt values
        for (int salt = 0; salt < numberOfHashes; salt++)
        {
            rbKey.setSalt(salt);
            context.write(rbKey, new IntWritable(BitmapIndex.getValueIndex(bucket, salt)));
            System.out.println("Outputting Key: " + rbKey + " | Value: " + BitmapIndex.getValueIndex(bucket, salt));
        }
	}
	
}
