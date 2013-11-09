package com._42six.amino.bitmap;

import com._42six.amino.common.*;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.datacache.BucketCache;
import com._42six.amino.common.service.datacache.BucketNameCache;
import com._42six.amino.common.service.datacache.DataSourceCache;
import com._42six.amino.common.service.datacache.VisibilityCache;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ByBucketMapper extends Mapper<BucketStripped, AminoWritable, ByBucketKey, BitmapValue> {

    private BucketCache bucketCache;
    private BucketNameCache bucketNameCache;
    private DataSourceCache dataSourceCache;
    private VisibilityCache visibilityCache;
    private int numberOfHashes;
    private int numberOfShards;

    private BucketStripped lastBS = new BucketStripped();
    private Bucket bucket;
    private ByBucketKey byBucketKey;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        final Configuration conf = context.getConfiguration();
        bucketCache = new BucketCache(conf);
        bucketNameCache = new BucketNameCache(conf);
        dataSourceCache = new DataSourceCache(conf);
        visibilityCache = new VisibilityCache(conf);
        numberOfHashes = conf.getInt("amino.bitmap.num-hashes", 1);
        numberOfShards = conf.getInt(BitmapConfigHelper.BITMAP_CONFIG_NUM_SHARDS, 10);
    }

    @Override
    protected void map(BucketStripped bucketStripped, AminoWritable aw, Context context) throws IOException, InterruptedException {

        // There are lots of repeated bucketStripped keys with different aw.  Cache results to save time/memory
        if(lastBS.compareTo(bucketStripped) != 0){
            lastBS = new BucketStripped(bucketStripped);

            bucket = bucketCache.getBucket(bucketStripped);
            final int binNumber = BitmapIndex.getBucketValueIndex(bucketStripped) % numberOfShards;
            final IntWritable bucketNameIndex = Preconditions.checkNotNull(bucketNameCache.getIndexForValue(bucket.getBucketName()));
            final IntWritable datasourceNameIndex = Preconditions.checkNotNull(dataSourceCache.getIndexForValue(bucket.getBucketDataSource()));
            final IntWritable visibilityIndex = Preconditions.checkNotNull(visibilityCache.getIndexForValue(bucket.getBucketVisibility()));
            byBucketKey = new ByBucketKey(bucket.getBucketValue(), binNumber, bucketNameIndex, datasourceNameIndex, visibilityIndex);
        }

        final Feature feature = aw.getFeature();
        final FeatureFact featureFact = aw.getFeatureFact();

        final BitmapValue bitmapValue = new BitmapValue();
        final Text saltText = new Text();

        for (int salt = 0; salt < numberOfHashes; salt++)
        {
            saltText.set(Integer.toString(salt));
            byBucketKey.setSalt(salt);
            bitmapValue.setIndex(BitmapIndex.getFeatureFactIndex(bucket, feature, featureFact, salt));
            context.write(byBucketKey, bitmapValue);
        }
    }
}
