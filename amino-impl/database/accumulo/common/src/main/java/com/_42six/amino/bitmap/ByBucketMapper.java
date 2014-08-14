package com._42six.amino.bitmap;

import com._42six.amino.common.*;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.datacache.BucketCache;
import com._42six.amino.common.service.datacache.SortedIndexCache;
import com._42six.amino.common.service.datacache.SortedIndexCacheFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ByBucketMapper extends Mapper<BucketStripped, AminoWritable, ByBucketKey, BitmapValue> {

    private BucketCache bucketCache;
    private SortedIndexCache bucketNameCache;
    private SortedIndexCache dataSourceCache;
    private SortedIndexCache visibilityCache;
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
        bucketNameCache = SortedIndexCacheFactory.getCache(SortedIndexCacheFactory.CacheTypes.BucketName, conf);
        dataSourceCache = SortedIndexCacheFactory.getCache(SortedIndexCacheFactory.CacheTypes.Datasource, conf);
        visibilityCache = SortedIndexCacheFactory.getCache(SortedIndexCacheFactory.CacheTypes.Visibility, conf);
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
            final int bucketNameIndex = bucketNameCache.getIndexForValue(bucket.getBucketName());
            if(bucketNameIndex < 0){
                throw new IOException("Could not find index in cache for bucket value: " + bucket.getBucketName());
            }
            final int datasourceNameIndex = dataSourceCache.getIndexForValue(bucket.getBucketDataSource());
            if(datasourceNameIndex < 0){
                throw new IOException("Could not find index in cache for datasource: " + bucket.getBucketDataSource());
            }
            final int visibilityIndex = visibilityCache.getIndexForValue(bucket.getBucketVisibility());
            if(visibilityIndex < 0){
                throw new IOException("Could not find index in cache for visibility: " + bucket.getBucketVisibility());
            }
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