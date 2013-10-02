package com._42six.amino.data.utilities;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com._42six.amino.api.job.AminoJob;
import com._42six.amino.common.Bucket;
import com._42six.amino.common.service.bucketcache.BucketCache;
import com._42six.amino.common.util.PathUtils;
import com._42six.amino.data.DataLoader;

public class BucketCacheBuilder {
	
	public static void buildBucketCache(DataLoader dataLoader, AminoJob job, String rootOutputPath, Configuration conf) throws IOException {
		PathUtils.setCachePath(conf, PathUtils.getJobCachePath(rootOutputPath));
		
		BucketCache writer = new BucketCache();
		
		String datasourceName = dataLoader.getDataSourceName();
		String visibility = dataLoader.getVisibility();
		String hrVisibility = dataLoader.getHumanReadableVisibility();
		
		Integer domainId = null;
		String domainName = null;
		String domainDescription = null;
		
		// Get domain info for this dataset
		if (job != null) {
			domainId = job.getAminoDomainID();
			domainName = job.getAminoDomainName();
			domainDescription = job.getAminoDomainDescription();
		}
		
		for(Text dataKey : dataLoader.getBuckets()) {
			String bucketName = dataKey.toString();
			Text displayName = dataLoader.getBucketDisplayNames().get(dataKey);
			
			Bucket bucket = new Bucket(datasourceName, bucketName, "", displayName == null ? null : displayName.toString(), visibility, hrVisibility);
			bucket.overrideBucketDataSourceWithDomain(domainId, domainName, domainDescription);
			writer.addBucket(bucket);
		}
		
		//write to disk and add to distributed cache
		writer.writeToDisk(conf, true);
	}

}
