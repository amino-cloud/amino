package com._42six.amino.bitmap;

import com._42six.amino.common.bitmap.AminoBitmap;
import com._42six.amino.common.bitmap.BitmapUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

public class BitLookupReducer extends Reducer<BitLookupKey, BitmapValue, Key, Value>
{
    @Override
    protected void reduce(BitLookupKey key, Iterable<BitmapValue> values, Context context) throws IOException, InterruptedException {
        final AminoBitmap bitmap = new AminoBitmap();
        final SortedSet<Integer> sortedBits = new TreeSet<Integer>();

        // The bits must be sorted first before they can be added to the AminoBitmap.
        for(BitmapValue value : values){
            sortedBits.addAll(value.getIndexes());
        }

        // Set the bits in the bitmap
        for (int index : sortedBits)
        {
            bitmap.set(index);
        }

        final Key outKey = new Key(Integer.toString(key.getFeatureIndex()), key.getFeatureFact(), Integer.toString(key.getSalt()), key.getVisibility());
        final Value outValue = BitmapUtils.toValue(bitmap);

        context.write(outKey, outValue);
    }
}
