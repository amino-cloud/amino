package com._42six.amino.bitmap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HypothesisReducer extends Reducer<Text, StatsKey, Key, Value>
{

    @Override
    protected void reduce(Text k, Iterable<StatsKey> values, Context context) throws IOException, InterruptedException
    {
        // Create one instance of each of the Text/Value so that we don't incur unnecessary class creation and gc
        final Value cbVal = new Value();

        for (StatsKey key : values)
        {
            final Key cbKey = new Key(Integer.toString(key.bitmapIndex),
                    Integer.toString(key.salt) + "#" + key.bucketName,
                    key.getRow(),
                    key.getVis());
            cbVal.set(key.getVal().getBytes());

            context.write(cbKey, cbVal);
        }
    }
}