package com._42six.amino.bitmap.reverse;

import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.bitmap.AminoBitmap;
import com._42six.amino.common.bitmap.BitmapUtils;
import com._42six.amino.common.service.datacache.SortedIndexCache;
import com._42six.amino.common.service.datacache.SortedIndexCacheFactory;
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

    private SortedIndexCache bucketNameCache;
    private SortedIndexCache dataSourceCache;
    private SortedIndexCache visibilityCache;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        final Configuration configuration = context.getConfiguration();
        String tableName = configuration.get(AminoConfiguration.TABLE_BUCKET);
        tableName = tableName.replace("amino_", "amino_reverse_") + AminoConfiguration.TEMP_SUFFIX;
        RB_BUCKET_TABLE = new Text(tableName);
        bucketNameCache = SortedIndexCacheFactory.getCache(SortedIndexCacheFactory.CacheTypes.BucketName, configuration);
        dataSourceCache = SortedIndexCacheFactory.getCache(SortedIndexCacheFactory.CacheTypes.Datasource, configuration);
        visibilityCache = SortedIndexCacheFactory.getCache(SortedIndexCacheFactory.CacheTypes.Visibility, configuration);
        super.setup(context);
    }

    @Override
    protected void reduce(ReverseBitmapKey rbk, Iterable<IntWritable> indexes, Context context) throws IOException, InterruptedException
    {
        final String datasource = dataSourceCache.getItem(rbk.getDatasource());
        final String bucketName = bucketNameCache.getItem(rbk.getBucketName());
        final String visibility = visibilityCache.getItem(rbk.getVisibility());

        final AminoBitmap bitmap = new AminoBitmap();
        final Mutation mutation = new Mutation(rbk.getShard() + ":" + rbk.getSalt());
        final ColumnVisibility colVis = new ColumnVisibility(visibility);

        final Text colFamily = new Text(datasource + "#" + bucketName + "#" + Integer.toString(rbk.getFeatureId()));
        final Text colQualifier = new Text(rbk.getFeatureValue());

        // Sort out all of the indexes since we can only add them to the bitmap in sorted order
        final TreeSet<Integer> sortedIndexes = new TreeSet<Integer>();

        // Gotta pull out the indexes because MR reuses the indexes
        for(IntWritable i : indexes){
            sortedIndexes.add(i.get());
        }

        // Add the bits to the bitmap
        for(Integer index : sortedIndexes){
            bitmap.set(index);
        }

        // Write the row out to the database
        mutation.put(colFamily, colQualifier, colVis, BitmapUtils.toValue(bitmap));
        context.write(RB_BUCKET_TABLE, mutation);
    }
}
