package com._42six.amino.bitmap.reverse;

import com._42six.amino.common.service.datacache.BucketNameCache;
import com._42six.amino.common.service.datacache.DataSourceCache;
import com._42six.amino.common.service.datacache.VisibilityCache;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class ReverseHypothesisReducer extends Reducer<Text, ReverseHypothesisValue, Key, Value>
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
	protected void reduce(Text k, Iterable<ReverseHypothesisValue> values, Context context) throws IOException, InterruptedException
    {
        final Text shard = new Text(k.toString().split(" ")[0]);
        
        for (ReverseHypothesisValue rhv : values)
        {
        	int index = rhv.getIndexPos();
        	int salt = rhv.getSalt();
            String DATASOURCE = dataSourceCache.getItem(rhv.getDatasourceIdx()).toString();
            String BUCKETNAME = bucketNameCache.getItem(rhv.getBucketNameIdx()).toString();
        	String BUCKET_VALUE = rhv.getBucketValue();
        	String VISIBILITY = visibilityCache.getItem(rhv.getVisibilityIdx()).toString();
        	
        	final Key cbKey = new Key(shard, new Text(Integer.toString(index) + "#" + DATASOURCE + "#" + BUCKETNAME + "#" + salt), new Text(BUCKET_VALUE), new Text(VISIBILITY));
        	final Value value = new Value("".getBytes());
        	context.write(cbKey, value);
        }
	}
}
