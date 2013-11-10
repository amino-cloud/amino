package com._42six.amino.bitmap.reverse;

import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.bitmap.AminoBitmap;
import com._42six.amino.common.bitmap.BitmapUtils;
import com._42six.amino.common.service.datacache.BucketNameCache;
import com._42six.amino.common.service.datacache.DataSourceCache;
import com._42six.amino.common.service.datacache.VisibilityCache;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class ReverseBitmapReducer extends Reducer<ReverseBitmapKey, IntWritable, Text, Mutation>
{
    private Text RB_BUCKET_TABLE;

    private BucketNameCache bucketNameCache;
    private DataSourceCache dataSourceCache;
    private VisibilityCache visibilityCache;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        final Configuration configuration = context.getConfiguration();
        String tableName = configuration.get("amino.bitmap.bucketTable");
        tableName = tableName.replace("amino_", "amino_reverse_") + IteratorUtils.TEMP_SUFFIX;
        RB_BUCKET_TABLE = new Text(tableName);
        bucketNameCache = new BucketNameCache(configuration);
        dataSourceCache = new DataSourceCache(configuration);
        visibilityCache = new VisibilityCache(configuration);
        super.setup(context);
    }

    @Override
    protected void reduce(ReverseBitmapKey rbk, Iterable<IntWritable> indexes, Context context) throws IOException, InterruptedException
    {
        final Text datasource = dataSourceCache.getItem(rbk.getDatasource());
        final Text bucketName = bucketNameCache.getItem(rbk.getBucketName());
        final Text visibility = visibilityCache.getItem(rbk.getVisibility());

        final AminoBitmap bitmap = new AminoBitmap();
        final Mutation mutation = new Mutation(rbk.getShard() + ":" + rbk.getSalt());
        final ColumnVisibility colVis = new ColumnVisibility(visibility);

        final Text colFamily = new Text(datasource + "#" + bucketName + "#" + Integer.toString(rbk.getFeatureId()));
        final Text colQualifier = new Text(rbk.getFeatureValue());

        // Sort out all of the indexes since we can only add them to the bitmap in sorted order
        final TreeSet<IntWritable> sortedIndexes = Sets.newTreeSet(indexes);

        // Add the bits to the bitmap
        for(IntWritable index : sortedIndexes){
            bitmap.set(index.get());
        }

        // Write the row out to the database
        mutation.put(colFamily, colQualifier, colVis, BitmapUtils.toValue(bitmap));
        context.write(RB_BUCKET_TABLE, mutation);
    }
}

