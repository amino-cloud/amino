package com._42six.amino.common.service.datacache;

import com._42six.amino.common.util.PathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.util.*;

/**
 *
 * @param <T>
 */
public abstract class SortedIndexCache<T extends WritableComparable> {

    /** The map indexed map of values */
    protected Map<IntWritable, T> dataMap = new HashMap<IntWritable, T>();
    protected String subFolder;

    public SortedIndexCache(String subFolder) {
        this.subFolder = subFolder;
    }

    public SortedIndexCache(String subFolder, Configuration conf) throws IOException {
        for (String cachePath : PathUtils.getCachePaths(conf)) {
            MapFile.Reader reader = null;
            try{
                reader = new MapFile.Reader(FileSystem.get(conf), cachePath + subFolder, conf);
                readFromDisk(reader);
            } finally {
                if(reader != null){
                    IOUtils.closeStream(reader);
                }
            }
        }
    }

    /** Method to read in the values from the HDFS mapfile.  Should be as simple as:
     * final IntWritable key = new IntWritable();
     * final T value = new T();
     * while(reader.next(key, value)){
     *     dataMap.put(new IntWritable(key.get()), new T(value));
     * }
     *
     * @param reader The MapFile.Reader to read values from
     */
    abstract protected void readFromDisk(final MapFile.Reader reader) throws IOException;

    /**
     * Persist the map to HDFS.
     * @param conf The Configuration
     */
    abstract protected void writeToDisk(Configuration conf) throws IOException;

    /**
     * Serialize the values out to HDFS.  NOTE! There is currently no synchronization method so make sure that you only
     * have one person writing to the cache and don't try to read while it's possible that the cache is being updated
     *
     * @param conf The job Configuration
     * @param writeToDistributedCache Whether or not to write to the DistributedCache
     */
    public void persist(Configuration conf, boolean writeToDistributedCache) throws IOException {
        final String cachePath = PathUtils.getCachePath(conf) + subFolder;
        final FileSystem fs = FileSystem.get(conf);

        System.out.println("Writing cache data to: " + cachePath);
        writeToDisk(conf);

        // TODO come back to this and make sure right
        if (writeToDistributedCache) {
            for (FileStatus status : fs.listStatus(new Path(cachePath))) {
                if (!status.isDir()) {
                    DistributedCache.addCacheFile(status.getPath().toUri(), conf);
                }
            }
        }

    }

    /**
     * Creates the map from the Set of items.  This will sort the items and then create an incremented index value for
     * each item in the collection
     * @param items The values to be placed in the map
     */
    public void setValues(Set<T> items){
        final TreeSet<T> sortedItems = new TreeSet<T>(items);
        setSortedValues(sortedItems);
    }

    /**
     * Adds the sorted values to the map.  An incremented index value will be created for each value
     * @param items
     */
    public void setSortedValues(SortedSet<T> items){
        dataMap = new HashMap<IntWritable, T>(items.size());

        int i = 0;
        for(T item : items){
            dataMap.put(new IntWritable(i), item);
            i++;
        }
    }

    /**
     * Returns the index for the given value
     * @param value The value to look up
     * @return The cache index for this value, null otherwise
     */
    public IntWritable getIndexForValue(T value){
        for(Map.Entry<IntWritable, T> entry : dataMap.entrySet()){
            if(entry.getValue().compareTo(value) == 0){
                return new IntWritable(entry.getKey().get());
            }
        }
        return null;
    }

    /**
     * Retrieves an item from the cache using its key
     * @param key The lookup key for the cache
     * @return The value associated with the key, or null if the key is not in the map
     */
    abstract public T getItem(IntWritable key);

    /**
     * Prints the contents of the cache to stdout
     */
    public void printCache(){
        for(Map.Entry<IntWritable, T> entry : dataMap.entrySet()){
            System.out.println(entry.getKey().toString() + " : " + entry.getValue().toString());
        }
    }

}
