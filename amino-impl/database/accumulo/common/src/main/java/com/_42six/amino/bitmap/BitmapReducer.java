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
	private boolean blastIndex;
    private String featureTable;
    private String bucketTable;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
        final Configuration conf = context.getConfiguration();
		blastIndex = conf.getBoolean("amino.bitmap.first.run", true);
        featureTable = conf.get(BitmapConfigHelper.AMINO_BITMAP_INDEX_TABLE);
        bucketTable = conf.get(BitmapConfigHelper.AMINO_BITMAP_BUCKET_TABLE);
	}

	@Override
	protected void reduce(BitmapKey key, Iterable<BitmapValue> values, Context context) throws IOException, InterruptedException 
	{
		final BitmapValue combinedValue = new BitmapValue();
		for (BitmapValue value : values) {
			combinedValue.merge(value);
		}

		final AminoBitmap bitmap = new AminoBitmap();
		for (int index : combinedValue.getIndexes())
		{
			bitmap.set(index);
		}

		final Mutation mutation = new Mutation(key.getRow());
		final ColumnVisibility cv = new ColumnVisibility(key.getVis().getBytes());
		mutation.put(key.getVal(), Integer.toString(key.salt), cv, BitmapUtils.toValue(bitmap));
		
		String table = getTable(key);
		if (blastIndex)
		{
			context.write(new Text(table + IteratorUtils.TEMP_SUFFIX), mutation);
		}
		else
		{
			context.write(new Text(table), mutation);
		}
	}
	
	private String getTable(BitmapKey key) throws IOException
	{
        switch (key.getType()) {
            case FEATURE:
                return featureTable;
            case BUCKET:
                return bucketTable;
            default:
                throw new IOException("Invalid key type");
        }
	}

}
