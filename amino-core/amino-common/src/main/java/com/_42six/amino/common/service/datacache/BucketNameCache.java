package com._42six.amino.common.service.datacache;

import com._42six.amino.common.util.PathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;

/**
 * A cache of all the different bucket names.  Helpful for compressing amount of data sent across the wire for MR jobs
 */
public class BucketNameCache extends SortedIndexCache<Text> {

    public static final String SUBDIR = "/bucketNames";

    public BucketNameCache() {
        super(SUBDIR);
    }

    public BucketNameCache(Configuration conf) throws IOException {
        super(SUBDIR, conf);
    }

    /**
     * Method to read in the values from the HDFS mapfile.
     *
     * @param reader The MapFile.Reader to read values from
     */
    @Override
    protected void readFromDisk(MapFile.Reader reader) throws IOException {
        final IntWritable key = new IntWritable();
        final Text value = new Text();
        while(reader.next(key, value)){
            dataMap.put(new IntWritable(key.get()), new Text(value));
        }
    }

    /**
     * Persist the map to HDFS.
     */
    @Override
    protected void writeToDisk(Configuration conf) throws IOException {
        final FileSystem fs = FileSystem.get(conf);
        MapFile.Writer writer = null;

        try {
            writer = new MapFile.Writer(conf, fs, PathUtils.getCachePath(conf) + subFolder, IntWritable.class, Text.class);

            for(Map.Entry<IntWritable, Text> entry : dataMap.entrySet()){
                writer.append(entry.getKey(), entry.getValue());
            }
        }
        finally {
            if (writer != null) {
                IOUtils.closeStream(writer);
            }
        }
    }

    /**
     * Retrieves an item from the cache using its key
     *
     * @param key The lookup key for the cache
     * @return The value associated with the key, or null if the key is not in the map
     */
    @Override
    public Text getItem(IntWritable key) {
        Text item = dataMap.get(key);
        return (item == null) ? item : new Text(item);
    }

}
