package com._42six.amino.bitmap;

import com._42six.amino.common.AminoConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StatsReducer extends Reducer<StatsKey, Text, Text, Mutation> 
{
	private boolean blastIndex = true;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		blastIndex = context.getConfiguration().getBoolean(AminoConfiguration.FIRST_RUN, true);
	}
	
	@Override
	protected void reduce(StatsKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		String table = context.getConfiguration().get(AminoConfiguration.TABLE_INDEX);
		
		String first = "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~";
	    String last = "                                  ";
		int count = 0;
		for (Text value : values)
		{
			count++;
			
			String test = value.toString();
			if (test.compareTo(first) < 0)
	  		{
	  			first = test;
	  		}
			if (test.compareTo(last) > 0)
			{
				last = test;
			}
		}
		
		ColumnVisibility cv = new ColumnVisibility(key.getVis().getBytes());
		Mutation m = new Mutation(key.getRow());
		m.put(key.getVal(),
				String.format("%s:COUNT", key.bucketName), cv, 
				Integer.toString(count));
		m.put(key.getVal(),
				String.format("%s:FIRST", key.bucketName), cv, 
				first);
		m.put(key.getVal(),
				String.format("%s:LAST", key.bucketName), cv, 
				last);

		if (blastIndex)
		{
			context.write(new Text(table + AminoConfiguration.TEMP_SUFFIX), m);
		}
		else
		{
			context.write(new Text(table), m);
		}
		
		
//		String featureLookupTable = context.getConfiguration().get(BitmapConfigHelper.AMINO_FEATURE_LOOKUP_TABLE);
//		Iterator<Integer> saltKeys = key.saltedIndexes.keySet().iterator();
//		while (saltKeys.hasNext())
//		{
//			int saltKey = saltKeys.next();
//			int index = key.saltedIndexes.get(saltKey);
//			
//			Mutation lookupMutation = new Mutation(new Text(Integer.toString(index)));
//			lookupMutation.put(Integer.toString(saltKey), key.bucketName, cv, String.format("%s:%s", key.getRow(), key.val));
//			
//			context.write(new Text(featureLookupTable + IteratorUtils.TEMP_SUFFIX), lookupMutation);
//		}
	}
}
