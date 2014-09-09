package com._42six.amino.bitmap;

import com._42six.amino.common.*;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.datacache.BucketCache;
import com._42six.amino.common.service.datacache.SortedIndexCache;
import com._42six.amino.common.service.datacache.SortedIndexCacheFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ByBucketMapper extends Mapper<BucketStripped, AminoWritable, ByBucketKey, BitmapValue> {

    private BucketCache bucketCache;
    private SortedIndexCache dataSourceCache;
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
        dataSourceCache = SortedIndexCacheFactory.getCache(SortedIndexCacheFactory.CacheTypes.Datasource, conf);
        numberOfHashes = conf.getInt(AminoConfiguration.NUM_HASHES, 1);
        numberOfShards = conf.getInt(AminoConfiguration.NUM_SHARDS, 10);
    }

    @Override
    protected void map(BucketStripped bucketStripped, AminoWritable aw, Context context) throws IOException, InterruptedException {

        // There are lots of repeated bucketStripped keys with different aw.  Cache results to save time/memory
        if(lastBS.compareTo(bucketStripped) != 0){
            lastBS = new BucketStripped(bucketStripped);

            bucket = bucketCache.getBucket(bucketStripped);
            final int binNumber = BitmapIndex.getBucketValueIndex(bucketStripped) % numberOfShards;
            final VIntWritable datasourceNameIndex = dataSourceCache.getIndexForValue(bucket.getBucketDataSource());
            if(datasourceNameIndex == null){
                throw new IOException("Could not find index in cache for datasource: " + bucket.getBucketDataSource());
            }
            final Text visibility = bucket.getBucketVisibility();
            byBucketKey = new ByBucketKey(bucket.getBucketValue(), binNumber, bucket.getBucketName(), datasourceNameIndex, visibility);
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