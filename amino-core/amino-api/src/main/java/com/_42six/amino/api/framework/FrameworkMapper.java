package com._42six.amino.api.framework;

import com._42six.amino.common.Bucket;
import com._42six.amino.common.BucketStripped;
import com._42six.amino.common.service.datacache.BucketCache;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public final class FrameworkMapper extends Mapper<MapWritable, MapWritable, BucketStripped, MapWritable> {

    public void map(MapWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
        // Go through available buckets. If this record has a value for that bucket, write the record to the context
        for (Bucket bucket : BucketCache.getBuckets(key)) {
            if (value.containsKey(bucket.getBucketName())) {
                Text bucketValue = (Text) value.get(bucket.getBucketName());
                Bucket bucketCopy = new Bucket(bucket);
                bucketCopy.setBucketValue(bucketValue);
                bucketCopy.computeHash();

                //Convert the full bucket to a light weight bucket for hdfs
                BucketStripped stripped = BucketStripped.fromBucket(bucketCopy);
                context.write(stripped, value);
            }
        }
    }
}
