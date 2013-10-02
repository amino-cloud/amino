package com._42six.amino.bitmap;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class BitmapCombiner extends Reducer<BitmapKey, BitmapValue, BitmapKey, BitmapValue> {
    @Override
    protected void reduce(BitmapKey key, Iterable<BitmapValue> values, Context context) throws IOException, InterruptedException {
        BitmapValue combinedValue = new BitmapValue();

        for (BitmapValue value : values) {
            combinedValue.merge(value);
        }

        context.write(key, combinedValue);
    }
}
