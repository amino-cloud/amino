package com._42six.amino.common.service.datacache;

import com._42six.amino.common.util.PathUtils;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

/**
 * A lookup table for helping to compress data in the MapReduce jobs.  This takes a SortedSet of String values, creates
 * an index number for each one, and persists the map to HDFS.  This allows the user to compress strings to integer values
 * and at the same time allows the the sorters to work properly by sorting the indexes based on how the underlying value
 * would have been lexigraphically sorted.
 *
 * NOTE!
 * Serializing the cache to HDFS is not thread safe.  You must do this as a singleton operation or you will get incorrect results
 */
public class SortedIndexCache  {

    /** The map indexed map of values */
    protected Map<IntWritable, String> dataMap = new HashMap<IntWritable, String>();
    protected String subFolder;

    public SortedIndexCache(String subFolder) {
        String sub = Preconditions.checkNotNull(subFolder);
        if(!sub.startsWith("/")){
            sub = "/" + sub;
        }
        this.subFolder = sub;
    }

    public SortedIndexCache(String subFolder, Configuration conf) throws IOException {
        this(subFolder);
        for (String cachePath : PathUtils.getCachePaths(conf)) {
            final FileSystem fs = FileSystem.get(conf);
            final String cacheFolder = PathUtils.concat(cachePath, subFolder);
            if(fs.exists(new Path(cacheFolder))){
                MapFile.Reader reader = null;
                try {
                    reader = new MapFile.Reader(FileSystem.get(conf), cacheFolder, conf);
                    readFromDisk(reader);
                } finally {
                    if(reader != null){
                        IOUtils.closeStream(reader);
                    }
                }
            } else {
                fs.mkdirs(new Path(cacheFolder));
            }
        }
    }

    /**
     * Method to read in the values from the HDFS mapfile.  Should be as simple as:
     * final IntWritable key = new IntWritable();
     * final T value = new T();
     * while(reader.next(key, value)){
     * dataMap.put(new IntWritable(key.get()), new T(value));
     * }
     *
     * @param reader The MapFile.Reader to read values from
     */
    protected void readFromDisk(MapFile.Reader reader) throws IOException {
        final IntWritable key = new IntWritable();
        final Text value = new Text();
        while(reader.next(key, value)){
            dataMap.put(new IntWritable(key.get()), value.toString());
        }
    }

    /**
     * Persist the map to HDFS.
     *
     * @param conf The Configuration
     */
    protected void writeToDisk(Configuration conf) throws IOException {
        final FileSystem fs = FileSystem.get(conf);
        MapFile.Writer writer = null;

        try {
            writer = new MapFile.Writer(conf, fs, PathUtils.getCachePath(conf) + subFolder, IntWritable.class, Text.class);

            final Text value = new Text();
            for(Map.Entry<IntWritable, String> entry : dataMap.entrySet()){
                value.set(entry.getValue());
                writer.append(entry.getKey(), value);
            }
        }
        finally {
            if (writer != null) {
                IOUtils.closeStream(writer);
            }
        }
    }

    /**
     * Serialize the values out to HDFS.  NOTE! There is currently no synchronization method so make sure that you only
     * have one person writing to the cache and don't try to read while it's possible that the cache is being updated
     *
     * @param conf The job Configuration
     * @param writeToDistributedCache Whether or not to write to the DistributedCache
     */
    public void persist(Configuration conf, boolean writeToDistributedCache) throws IOException {
        final String cachePath = PathUtils.concat(PathUtils.getCachePath(conf), subFolder);
        final FileSystem fs = FileSystem.get(conf);

        System.out.println("Writing cache data to: " + cachePath);
        writeToDisk(conf);

        // TODO come back to this and make sure right
        if (writeToDistributedCache) {
            for (FileStatus status : fs.listStatus(new Path(cachePath))) {
                if (!status.isDirectory()) {
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
    public void setValues(Set<String> items){
        final TreeSet<String> sortedItems = new TreeSet<String>(items);
        setSortedValues(sortedItems);
    }

    /**
     * Adds the sorted values to the map.  An incremented index value will be created for each value
     * @param items
     */
    public void setSortedValues(SortedSet<String> items){
        dataMap = new HashMap<IntWritable, String>(items.size());

        int i = 0;
        for(String item : items){
            dataMap.put(new IntWritable(i), item);
            i++;
        }
    }

    /**
     * Returns the index for the given value
     * @param value The value to look up
     * @return The cache index for this value, Integer.MIN_VALUE otherwise
     */
    public int getIndexForValue(String value){
        for(Map.Entry<IntWritable, String> entry : dataMap.entrySet()){
            if(entry.getValue().compareTo(value) == 0){
                // Return the int instead of the key so that users don't accidentally modify the key
                return entry.getKey().get();
            }
        }
        return Integer.MIN_VALUE;
    }

    public int getIndexForValue(Text value){
        return getIndexForValue(value.toString());
    }

    /**
     * Retrieves an item from the cache using its key
     * @param key The lookup key for the cache
     * @return The value associated with the key, or null if the key is not in the map
     */
    public String getItem(IntWritable key){
        // Strings are immutable so we can just return the
        return dataMap.get(key);
    }

    /**
     * Prints the contents of the cache to stdout
     */
    public void printCache(){
        for(Map.Entry<IntWritable, String> entry : dataMap.entrySet()){
            System.out.println(entry.getKey().toString() + " : " + entry.getValue());
        }
    }

}
