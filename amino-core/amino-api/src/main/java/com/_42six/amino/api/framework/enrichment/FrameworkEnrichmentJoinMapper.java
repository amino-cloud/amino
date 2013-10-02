package com._42six.amino.api.framework.enrichment;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com._42six.amino.api.framework.AminoDriverUtils;
import com._42six.amino.common.Bucket;
import com._42six.amino.common.BucketStripped;
import com._42six.amino.common.service.bucketcache.BucketCache;
import com._42six.amino.data.AminoDataUtils;
import com._42six.amino.data.AminoRecordReader;
import com._42six.amino.data.DataLoader;
import com._42six.amino.data.EnrichWorker;


public class FrameworkEnrichmentJoinMapper extends Mapper<MapWritable, MapWritable, EnrichmentJoinKey, MapWritable> 
{
	private EnrichWorker ew;
	private Iterable<DataLoader> joinLoaders;
	
	private static final Text DATASOURCE_KEY = new Text(AminoRecordReader.DATA_SOURCE_NAME_KEY);
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        
        try {
        	ew = AminoDataUtils.getEnrichWorker(context.getConfiguration());
        	joinLoaders = AminoDataUtils.getJoinDataLoaders(context.getConfiguration());
        }
        catch (Exception ex) {
        	ex.printStackTrace();
        	throw new IOException(ex);
        }
    }
	
	@Override
	public void map(MapWritable key, MapWritable value, Context context) throws IOException, InterruptedException 
	{		
		Iterable<String> joinKeys;
		int type = -1;
		
		for (DataLoader joinLoader : joinLoaders) {
			if (joinLoader.getDataSourceName().equals(key.get(DATASOURCE_KEY).toString())) {
				joinKeys = ew.getEnrichmentKey(value);
				if (joinKeys != null) {
					type = EnrichmentJoinKey.TYPE_ENRICHMENT;
					
					for (String joinKey : joinKeys) {
						context.write(new EnrichmentJoinKey(joinKey, type), value);
					}
					break;
				}
			}
		}
		
		if (type == -1) {
			joinKeys = ew.getSubjectKey(value);
			if (joinKeys != null) {
				type = EnrichmentJoinKey.TYPE_SUBJECT;
				
				Collection<Bucket> bucketList = BucketCache.getBuckets(key);
				
				for (Bucket bucket : bucketList) {
					if (value.containsKey(bucket.getBucketName())) {
						Text bucketValue = (Text)value.get(bucket.getBucketName());
						Bucket bucketCopy = new Bucket(bucket);
						bucketCopy.setBucketValue(bucketValue);
						bucketCopy.computeHash();
						BucketStripped stripped = BucketStripped.fromBucket(bucketCopy);
						
						for (String joinKey : joinKeys) {
							value.put(new Text(AminoDriverUtils.AMINO_ENRICHMENT_BUCKET), stripped);
							EnrichmentJoinKey ejk = new EnrichmentJoinKey(joinKey, type);
							//Setting the bucket in the key will do the secondary sorting in the reduce step
							ejk.setBucket(stripped);
							context.write(ejk, value);
						}
					}
				}
			}
		}
	}
}
