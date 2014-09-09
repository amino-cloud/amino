package com._42six.amino.common.service.datacache;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Creates a SortedIndexCache for the particular type of information
 */
public class SortedIndexCacheFactory {
    public enum CacheTypes{
        Datasource,
    }

    /**
     * Produces the SortedIndexCache for the particular type
     * @param type The data type of the cache to instantiate
     * @param conf The Hadoop Configuration
     * @return The appropriate cache for the type of data requested
     */
    public static SortedIndexCache getCache(CacheTypes type, Configuration conf) throws IOException {
        final SortedIndexCache retVal;
        switch (type){
            case Datasource:
                retVal = new SortedIndexCache("/dataSources", conf);
                break;
            default:
                throw new NotImplementedException("Cache type " + type + " is not implemented yet");
        }
        retVal.loadFromStorage();
        return retVal;
    }
}
