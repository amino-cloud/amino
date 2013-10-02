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
		final Text row = new Text();
		final Text cf = new Text();
		final Text cq = new Text();
		final Text vis = new Text();
		final Value btVal = new Value();

		for (StatsKey key : values)
		{
			// Set the values for the current Entry
			row.set(Integer.toString(key.bitmapIndex));
			cf.set(Integer.toString(key.salt) + "#" + key.bucketName);
			cq.set(key.getRow());
			vis.set(key.getVis());

//			Key btKey = new Key(new Text(Integer.toString(index)), new Text(Integer.toString(saltKey)), new Text(key.bucketName), new Text(key.getVis()));
//			Value btVal = new Value();
//			btVal.set(String.format("%s:%s", key.getRow(), key.val).getBytes());

			Key btKey = new Key(row, cf, cq, vis);
			btVal.set(key.getVal().getBytes());

			context.write(btKey, btVal);
		}
		
//		TreeMap<Key,Value> sorted = new TreeMap<Key,Value>();
//		for (StatsKey key : values)
//		{
//			int saltKey = key.salt;
//			int index = key.bitmapIndex;
//
//			Key btKey = new Key(new Text(Integer.toString(index)), new Text(Integer.toString(saltKey)), new Text(key.bucketName), new Text(key.getVis()));
//			Value btVal = new Value();
//			btVal.set(String.format("%s:%s", key.getRow(), key.val).getBytes());
//
//			sorted.put(btKey, btVal);
//		}
//		
//		Iterator<Key> keys = sorted.keySet().iterator();
//		while(keys.hasNext())
//		{
//			Key btKey = keys.next();
//			Value btVal = sorted.get(btKey);
//			context.write(btKey, btVal);
//		}
	}

}
