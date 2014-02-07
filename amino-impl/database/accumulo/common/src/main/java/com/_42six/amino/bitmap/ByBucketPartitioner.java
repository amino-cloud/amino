package com._42six.amino.bitmap;

import com._42six.amino.common.ByBucketKey;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class ByBucketPartitioner extends Partitioner<ByBucketKey, Writable> {

    private final RangePartitioner rp = new RangePartitioner();
    private final Text key = new Text();

    @Override
    public int getPartition(ByBucketKey byBucketKey, Writable bitmapValue, int numPartitions) {
        key.set(byBucketKey.getBinNumber() + ":" + byBucketKey.getSalt());
        return rp.getPartition(key, bitmapValue, numPartitions);
    }

    public Configuration getConf(){
        return rp.getConf();
    }

    public void setConf(Configuration conf){
        rp.setConf(conf);
    }

    public static void setSplitFile(Job job, String file){
        RangePartitioner.setSplitFile(job, file);
    }

    public static void setNumSubBins(Job job, int num){
        RangePartitioner.setNumSubBins(job, num);
    }
}