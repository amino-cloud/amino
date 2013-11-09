package com._42six.amino.bitmap.reverse;

import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.bitmap.AminoBitmap;
import com._42six.amino.common.bitmap.BitmapUtils;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class ReverseBitmapReducer extends Reducer<ReverseBitmapKey, IntWritable, Text, Mutation>
{
    private Text RB_BUCKET_TABLE;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        String tableName = context.getConfiguration().get("amino.bitmap.bucketTable");
        tableName = tableName.replace("amino_", "amino_reverse_");
        tableName = tableName + IteratorUtils.TEMP_SUFFIX;
        RB_BUCKET_TABLE = new Text(tableName);
        super.setup(context);
    }

    @Override
    protected void reduce(ReverseBitmapKey rbk, Iterable<IntWritable> indexes, Context context) throws IOException, InterruptedException
    {
        final AminoBitmap bitmap = new AminoBitmap();
        final Mutation mutation = new Mutation(rbk.getShard() + ":" + rbk.getSalt());
        final ColumnVisibility colVis = new ColumnVisibility(rbk.getVisibility().getBytes());
        final Text colFamily = new Text(rbk.getDatasource() + "#" + rbk.getBucketName() + "#" + Integer.toString(rbk.getFeatureId()));
        final Text colQualifier = new Text(rbk.getFeatureValue());
        final TreeSet<Integer> sortedIndexes = new TreeSet<Integer>();

        // Sort out all of the indexes since we can only add them to the bitmap in sorted order
        int i = 0;
        for(IntWritable index : indexes){
            i++;
            sortedIndexes.add(index.get());
        }

        // Add the bits to the bitmap
        for(int index : sortedIndexes){
            bitmap.set(index);
        }

        // Write the row out to database
        mutation.put(colFamily, colQualifier, colVis, BitmapUtils.toValue(bitmap));
        context.write(RB_BUCKET_TABLE, mutation);
    }
}
