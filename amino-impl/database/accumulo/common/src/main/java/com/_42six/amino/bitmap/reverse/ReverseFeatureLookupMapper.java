package com._42six.amino.bitmap.reverse;

import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Bucket;
import com._42six.amino.common.BucketStripped;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.datacache.BucketCache;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReverseFeatureLookupMapper extends Mapper<BucketStripped, AminoWritable, Key, Value>
{
    private BucketCache bucketCache;
    private BucketStripped lastBS;

    private int numberOfShards;
    private int numberOfHashes;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        final Configuration conf = context.getConfiguration();
        bucketCache = new BucketCache(conf);

        numberOfShards = context.getConfiguration().getInt(AminoConfiguration.NUM_SHARDS, 10);
        numberOfHashes = context.getConfiguration().getInt(AminoConfiguration.NUM_HASHES, 1);
    }

    @Override
    protected void map(BucketStripped bs, AminoWritable aw, Context context) throws IOException, InterruptedException
    {
        // Note: We currently don't care what the aw value is as we don't need it for anything

        /**
         * TODO: We get a whole bunch of bucket/bucket Value that are exactly the same.  See if it'd be worth our time
         * to write out a different dataset to read from instead of reading all of these duplicate values and acting on them
         */
        if(lastBS == null){
            lastBS = new BucketStripped(bs);
        } else {
            if(lastBS.compareTo(bs) != 0){
                lastBS = new BucketStripped(bs);
            } else {
                // Already seen this value, move on
                return;
            }
        }

        final Bucket bucket = bucketCache.getBucket(bs);
        final Value value = new Value("".getBytes());

        // Make sure that we have the same shard for all of the salts
        int index = BitmapIndex.getValueIndex(bucket, 0);
        final Text SHARD = new Text(Integer.toString(index % numberOfShards));

        // Write the Key/Value for each salt
        for (int salt = 0; salt < numberOfHashes; salt++)
        {
            index = BitmapIndex.getValueIndex(bucket, salt);
            final Key cbKey = new Key(SHARD, new Text(Integer.toString(index) + "#" + bucket.getBucketDataSource() + "#" + bucket.getBucketName() + "#" + salt),
                    bucket.getBucketValue(), bucket.getBucketVisibility());
            context.write(cbKey, value);
        }
    }
}