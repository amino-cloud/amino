package com._42six.amino.common.service.datacache;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Creates a SortedIndexCache for the particular type of information
 */
public class SortedIndexCacheFactory {
    public enum CacheTypes{
        BucketName,
        Datasource,
        Visibility
    }

    /**
     * Produces the SortedIndexCache for the particular type
     * @param type The data type of the cache to instantiate
     * @return The appropriate cache for the type of data requested
     */
    public static SortedIndexCache getCache(CacheTypes type){
        switch (type){
            case BucketName:
                return new SortedIndexCache("/bucketNames");
            case Datasource:
                return new SortedIndexCache("/dataSources");
            case Visibility:
                return new SortedIndexCache("/visibilities");
        }
        throw new NotImplementedException("Cache type " + type + " is not implemented yet");
    }

    /**
     * Produces the SortedIndexCache for the particular type
     * @param type The data type of the cache to instantiate
     * @param conf The Hadoop Configuration
     * @return The appropriate cache for the type of data requested
     */
    public static SortedIndexCache getCache(CacheTypes type, Configuration conf) throws IOException {
        switch (type){
            case BucketName:
                return new SortedIndexCache("/bucketNames", conf);
            case Datasource:
                return new SortedIndexCache("/dataSources", conf);
            case Visibility:
                return new SortedIndexCache("/visibilities", conf);
        }
        throw new NotImplementedException("Cache type " + type + " is not implemented yet");
    }
}
