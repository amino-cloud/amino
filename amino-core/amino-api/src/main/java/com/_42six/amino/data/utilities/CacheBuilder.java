package com._42six.amino.data.utilities;

import com._42six.amino.api.job.AminoJob;
import com._42six.amino.common.Bucket;
import com._42six.amino.common.service.datacache.BucketCache;
import com._42six.amino.common.service.datacache.SortedIndexCache;
import com._42six.amino.common.service.datacache.SortedIndexCacheFactory;
import com._42six.amino.common.util.PathUtils;
import com._42six.amino.data.DataLoader;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.util.SortedSet;
import java.util.TreeSet;

public class CacheBuilder {
	
	public static void buildCaches(DataLoader dataLoader, AminoJob job, String rootOutputPath, Configuration conf) throws Exception {
		PathUtils.setCachePath(conf, PathUtils.getJobCachePath(rootOutputPath));
		
		final BucketCache bucketCache = new BucketCache();
        final SortedIndexCache bucketNameCache = SortedIndexCacheFactory.getCache(SortedIndexCacheFactory.CacheTypes.BucketName, conf);
        final SortedIndexCache dataSourceCache = SortedIndexCacheFactory.getCache(SortedIndexCacheFactory.CacheTypes.Datasource, conf);
        final SortedIndexCache visibilityCache = SortedIndexCacheFactory.getCache(SortedIndexCacheFactory.CacheTypes.Visibility, conf);

		final String datasourceName = dataLoader.getDataSourceName();
		final String visibility = dataLoader.getVisibility();
		final String hrVisibility = dataLoader.getHumanReadableVisibility();
		
		Integer domainId = null;
		String domainName = null;
		String domainDescription = null;
        final SortedSet<String> bucketNames = new TreeSet<>();

		// Get domain info for this dataset
		if (job != null) {
			domainId = job.getAminoDomainID();
			domainName = job.getAminoDomainName();
			domainDescription = job.getAminoDomainDescription();
		}

		for(Text dataKey : dataLoader.getBuckets()) {
            bucketNames.add(dataKey.toString());
			Text displayName = dataLoader.getBucketDisplayNames().get(dataKey);
			
			Bucket bucket = new Bucket(datasourceName, dataKey.toString(), "", displayName == null ? null : displayName.toString(), visibility, hrVisibility);
			bucket.overrideBucketDataSourceWithDomain(domainId, domainName, domainDescription);
			bucketCache.addBucket(bucket);
		}
		
		// write to disk and add to distributed cache
		bucketCache.writeToDisk(conf, true);

        // Write the cached data to the file system and then when
        // we use the BucketMapper, we can write out the names as an index (saving space) and providing ordering so that
        // we don't have to use a Key, and instead can use a ByBucketKey which can be sorted

        bucketNameCache.addValues(bucketNames);
        dataSourceCache.addValues(Sets.newHashSet((domainId != null) ? domainId.toString() : datasourceName));
        visibilityCache.addValues(Sets.newHashSet(visibility));
        bucketNameCache.persist();
        dataSourceCache.persist();
        visibilityCache.persist();
	}

}
