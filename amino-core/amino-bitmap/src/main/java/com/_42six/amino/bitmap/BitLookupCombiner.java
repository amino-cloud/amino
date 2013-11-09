package com._42six.amino.bitmap;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BitLookupCombiner extends Reducer<BitLookupKey, BitmapValue, BitLookupKey, BitmapValue> {
    @Override
    protected void reduce(BitLookupKey key, Iterable<BitmapValue> values, Context context) throws IOException, InterruptedException {
        final BitmapValue combinedValue = new BitmapValue();

        for (BitmapValue value : values) {
            combinedValue.merge(value);
        }

        context.write(key, combinedValue);
    }
}
