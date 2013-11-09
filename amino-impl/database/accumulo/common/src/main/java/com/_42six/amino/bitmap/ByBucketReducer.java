package com._42six.amino.bitmap;

import com._42six.amino.common.ByBucketKey;
import com._42six.amino.common.bitmap.AminoBitmap;
import com._42six.amino.common.bitmap.BitmapUtils;
import com._42six.amino.common.service.datacache.BucketNameCache;
import com._42six.amino.common.service.datacache.DataSourceCache;
import com._42six.amino.common.service.datacache.VisibilityCache;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

public class ByBucketReducer extends Reducer<ByBucketKey, BitmapValue, Key, Value>
{
    private BucketNameCache bucketNameCache;
    private DataSourceCache dataSourceCache;
    private VisibilityCache visibilityCache;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        final Configuration conf = context.getConfiguration();
        bucketNameCache = new BucketNameCache(conf);
        dataSourceCache = new DataSourceCache(conf);
        visibilityCache = new VisibilityCache(conf);
    }

    @Override
    protected void reduce(ByBucketKey key, Iterable<BitmapValue> values, Context context) throws IOException, InterruptedException
    {
        final SortedSet<Integer> sortedBits = new TreeSet<Integer>();

        // The bits must be sorted first before they can be added to the AminoBitmap.
        for(BitmapValue value : values){
            sortedBits.addAll(value.getIndexes());
        }

        final AminoBitmap bitmap = new AminoBitmap();
        for (int index : sortedBits)
        {
            bitmap.set(index);
        }

        final int binNumber = key.getBinNumber();
        final String dataSource = dataSourceCache.getItem(key.getDatasourceNameIndex()).toString();
        final String bucketName = bucketNameCache.getItem(key.getBucketNameIndex()).toString();
        final String bucketValue = key.getBucketValue().toString();
        final String vis = visibilityCache.getItem(key.getVisibilityIndex()).toString();

        final Key outKey = new Key(String.format("%d:%s:%s", binNumber, dataSource, bucketName), bucketValue,
                Integer.toString(key.getSalt()), vis);

        context.write(outKey, BitmapUtils.toValue(bitmap));
    }
}
