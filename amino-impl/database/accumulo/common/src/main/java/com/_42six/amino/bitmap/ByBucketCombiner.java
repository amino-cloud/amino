package com._42six.amino.bitmap;

import com._42six.amino.common.ByBucketKey;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ByBucketCombiner extends Reducer<ByBucketKey, BitmapValue, ByBucketKey, BitmapValue> {
    @Override
    protected void reduce(ByBucketKey key, Iterable<BitmapValue> values, Context context) throws IOException, InterruptedException {
        final BitmapValue combinedValue = new BitmapValue();

        for (BitmapValue value : values) {
            combinedValue.merge(value);
        }

        context.write(key, combinedValue);
    }
}