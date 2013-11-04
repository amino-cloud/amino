package com._42six.amino.bitmap;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BucketCombiner extends Reducer<BucketKey, BitmapValue, BucketKey, BitmapValue> {
    @Override
    protected void reduce(BucketKey key, Iterable<BitmapValue> values, Context context) throws IOException, InterruptedException {
        BitmapValue combinedValue = new BitmapValue();

        for (BitmapValue value : values) {
            combinedValue.merge(value);
        }

        context.write(key, combinedValue);
    }
}
