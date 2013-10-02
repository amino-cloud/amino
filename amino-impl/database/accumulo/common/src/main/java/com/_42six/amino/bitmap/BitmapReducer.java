package com._42six.amino.bitmap;

import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.bitmap.AminoBitmap;
import com._42six.amino.common.bitmap.BitmapUtils;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BitmapReducer extends Reducer<BitmapKey, BitmapValue, Text, Mutation> 
{
	private boolean blastIndex = true;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		blastIndex = context.getConfiguration().getBoolean("amino.bitmap.first.run", true);
	}

	@Override
	protected void reduce(BitmapKey key, Iterable<BitmapValue> values, Context context) throws IOException, InterruptedException 
	{
		BitmapValue combinedValue = new BitmapValue();
		for (BitmapValue value : values) {
			combinedValue.merge(value);
		}

		AminoBitmap bitmap = new AminoBitmap();
		for (int index : combinedValue.getIndexes()) 
		{
			bitmap.set(index);
		}
		Mutation mutation = new Mutation(key.getRow());
		ColumnVisibility cv = new ColumnVisibility(key.getVis().getBytes());
		mutation.put(key.getVal(), Integer.toString(key.salt), cv, BitmapUtils.toValue(bitmap));
		
		String table = getTable(context, key);
		if (blastIndex)
		{
			context.write(new Text(table + IteratorUtils.TEMP_SUFFIX), mutation);
		}
		else
		{
			context.write(new Text(table), mutation);
		}
	}
	
	public String getTable(Context context, BitmapKey key) throws IOException
	{
		String table = "";
		Configuration conf = context.getConfiguration();
        switch (key.getType()) {
            case FEATURE:
                table = conf.get(BitmapConfigHelper.AMINO_BITMAP_INDEX_TABLE);
                break;
            case BUCKET:
                table = conf.get(BitmapConfigHelper.AMINO_BITMAP_BUCKET_TABLE);
                break;
            default:
                throw new IOException("Invalid key type");
        }
		
		return table;
	}

}
