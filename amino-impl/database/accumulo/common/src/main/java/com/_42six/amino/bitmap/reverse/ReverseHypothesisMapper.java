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
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReverseHypothesisMapper extends Mapper<BucketStripped, AminoWritable, Text, ReverseHypothesisValue>
{
	private BucketCache bucketCache;
    private BucketNameCache bucketNameCache;
    private DataSourceCache dataSourceCache;
    private VisibilityCache visibilityCache;
    private BucketStripped lastBS;

    private int numberOfShards;
    private int numberOfHashes;

    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
        final Configuration conf = context.getConfiguration();
    	bucketCache = new BucketCache(conf);
        bucketNameCache = new BucketNameCache(conf);
        dataSourceCache = new DataSourceCache(conf);
        visibilityCache = new VisibilityCache(conf);

		numberOfShards = context.getConfiguration().getInt(BitmapConfigHelper.BITMAP_CONFIG_NUM_SHARDS, 10);
	    numberOfHashes = context.getConfiguration().getInt("amino.bitmap.num-hashes", 1);
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
        final IntWritable BUCKETNAME = bucketNameCache.getIndexForValue(bucket.getBucketName());
        final IntWritable DATASOURCE = dataSourceCache.getIndexForValue(bucket.getBucketDataSource());
        final IntWritable VISIBILITY = visibilityCache.getIndexForValue(bucket.getBucketVisibility());
        final String BUCKET_VALUE = bucket.getBucketValue().toString();

        // Make sure that we have the same shard for all of the salts
        int index = BitmapIndex.getValueIndex(bucket, 0);
        final Text SHARD = new Text(Integer.toString(index % numberOfShards));

        final ReverseHypothesisValue rhv = new ReverseHypothesisValue(index, DATASOURCE, BUCKETNAME, 0, BUCKET_VALUE, VISIBILITY);

        // Write the Key/Value for each salt
        for (int salt = 0; salt < numberOfHashes; salt++)
        {
        	index = BitmapIndex.getValueIndex(bucket, salt);
            final Key cbKey = new Key(SHARD, new Text(Integer.toString(index) + "#" + bucket.getBucketDataSource() + "#" + bucket.getBucketName() + "#" + salt),
                    bucket.getBucketValue(), bucket.getBucketVisibility());
        	rhv.setSalt(salt);
            rhv.setIndexPos(index);
            context.write(new Text(cbKey.toStringNoTime()), rhv);
        }
	}
}
