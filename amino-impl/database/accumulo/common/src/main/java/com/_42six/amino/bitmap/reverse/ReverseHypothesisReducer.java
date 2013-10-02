package com._42six.amino.bitmap.reverse;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//public class ReverseHypothesisReducer extends Reducer<Key, ReverseHypothesisBitmapValue, Key, Value>
//public class ReverseHypothesisReducer extends Reducer<Key, NullWritable, Key, Value>
public class ReverseHypothesisReducer extends Reducer<Key, NullWritable, Text, Mutation>
{
	@Override
	//protected void reduce(Text k, Iterable<ReverseHypothesisBitmapValue> values, Context context) throws IOException, InterruptedException
    protected void reduce(Key k, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
    {
        /*
        Assumes that keys are in the form shard#salt index bucketName and the value is the bucketValue
         */
        System.out.println("Parsing Key: " + k.toString());
//
//		String[] row = k.toString().split(" ");
//        final Text shardSalt = new Text(row[0]);
//        final Text bucketValueIndex = new Text(row[1]);
//        final Text bucketName = new Text(row[2]);

//		for (ReverseHypothesisBitmapValue rhbv : values)
//		{
//            System.out.println("Reducing Value: " + rhbv.toString());
////			String[] parts = row[1].split(rhbv.getBucketName() + "#");
////			String salt = parts[1];
//
//			final Key cbKey = new Key(shardSalt, bucketValueIndex, bucketName, new Text(rhbv.getVisibility()));
//			final Value cbVal = new Value(rhbv.getBucketValue().getBytes());
//
//			context.write(cbKey, cbVal);
//            System.out.println("Writing Key: " + cbKey.toString() + " | Value: " + cbVal.toString());
//		}

        Mutation m = new Mutation(k.getRow());
//        m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility("U"), new Value("test".getBytes()));
        m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), new Value("".getBytes()));
        context.write(new Text("amino_reverse_feature_lookup_numbers_temp"), m);

        //context.write(k, new Value("test".getBytes()));
	}
}
