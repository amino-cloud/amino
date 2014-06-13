package com._42six.amino.bitmap;

import com._42six.amino.common.*;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.service.datacache.BucketCache;
import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class HypothesisMapper extends Mapper<BucketStripped, AminoWritable, Text, StatsKey>
{

    private BucketCache bucketCache;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        bucketCache = new BucketCache(context.getConfiguration());
    }

    @Override
    protected void map(BucketStripped bucketStripped, AminoWritable aw, Context context) throws IOException, InterruptedException
    {
        final Bucket bucket = bucketCache.getBucket(bucketStripped);

        final int numberOfHashes = context.getConfiguration().getInt(AminoConfiguration.NUM_HASHES, 1);

        final Feature feature = aw.getFeature();
        final FeatureFact featureFact = aw.getFeatureFact();
        final int featureIndex = BitmapIndex.getFeatureIndex(feature);

        final Text bucketName = bucket.getBucketName();
        final Text bucketVis  = bucket.getBucketVisibility();
        final Text row = new Text();
        final Text cf = new Text();

        for (int salt = 0; salt < numberOfHashes; salt++)
        {
            int featureFactIndex = BitmapIndex.getFeatureFactIndex(bucket, feature, featureFact, salt);
            row.set(Integer.toString(featureFactIndex));
            cf.set(Integer.toString(salt) + "#" + bucketName);

            StatsKey featureKey = new StatsKey(Integer.toString(featureIndex),
                    featureFact.toText(new FeatureFactTranslatorImpl()).toString(),
                    bucketName.toString(),
                    bucketVis.toString(),
                    salt,
                    featureFactIndex);

            Key cbKey = new Key(row, cf, new Text(Integer.toString(featureIndex)), bucketVis);

            context.write(new Text(cbKey.toStringNoTime()), featureKey);
        }
    }

}
