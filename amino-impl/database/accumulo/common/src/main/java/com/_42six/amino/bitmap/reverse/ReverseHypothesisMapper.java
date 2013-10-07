package com._42six.amino.bitmap.reverse;

import com._42six.amino.bitmap.BitmapConfigHelper;
import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Bucket;
import com._42six.amino.common.BucketStripped;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.bucketcache.BucketCache;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;

//public class ReverseHypothesisMapper extends Mapper<BucketStripped, AminoWritable, Text, ReverseHypothesisBitmapValue>
public class ReverseHypothesisMapper extends Mapper<BucketStripped, AminoWritable, Text, ReverseHypothesisValue>
{
	private BucketCache bucketCache;
    private static BucketStripped lastBS;

    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
    	bucketCache = new BucketCache(context.getConfiguration());
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
        final String BUCKETNAME = bucket.getBucketName().toString();
        final String DATASOURCE = bucket.getBucketDataSource().toString();

		final int numberOfShards = context.getConfiguration().getInt(BitmapConfigHelper.BITMAP_CONFIG_NUM_SHARDS, 10);
	    final int numberOfHashes = context.getConfiguration().getInt("amino.bitmap.num-hashes", 1);

        // Make sure that we have the same shard for all of the salts
        int index = BitmapIndex.getValueIndex(bucket, 0);
        final int shard = index % numberOfShards;
        final Text SHARD = new Text(Integer.toString(shard));
        final Text VISIBILITY = bucket.getBucketVisibility();

        //final ReverseHypothesisBitmapValue rhbv = new ReverseHypothesisBitmapValue(index, BUCKETNAME, bucket.getBucketValue().toString(), VISIBILITY.toString());
        final String BUCKET_VALUE = bucket.getBucketValue().toString();
        ReverseHypothesisValue rhv = new ReverseHypothesisValue(index, DATASOURCE, BUCKETNAME, 0, BUCKET_VALUE, VISIBILITY.toString());

        Key cbKey = new Key(SHARD, new Text(Integer.toString(index) + "#" + DATASOURCE + "#" + BUCKETNAME + "#0"), new Text(BUCKET_VALUE), VISIBILITY);
        //context.write(cbKey, NullWritable.get());
        context.write(new Text(cbKey.toStringNoTime()), rhv);

        // Do the rest of the salts
        for (int salt = 1; salt < numberOfHashes; salt++)
        {
        	index = BitmapIndex.getValueIndex(bucket, salt);
            cbKey = new Key(SHARD, new Text(Integer.toString(index) + "#" + DATASOURCE + "#" + BUCKETNAME + "#" + salt), new Text(BUCKET_VALUE), VISIBILITY);
        	rhv = new ReverseHypothesisValue(index, DATASOURCE, BUCKETNAME, salt, BUCKET_VALUE, VISIBILITY.toString());
            //context.write(cbKey, NullWritable.get());
            context.write(new Text(cbKey.toStringNoTime()), rhv);
        }
	}
}
