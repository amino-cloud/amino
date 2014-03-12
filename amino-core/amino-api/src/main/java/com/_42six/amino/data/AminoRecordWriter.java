package com._42six.amino.data;

import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Bucket;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class AminoRecordWriter extends RecordWriter<Bucket, AminoWritable> {

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
        // EMPTY
    }

    @Override
    public void write(Bucket bucket, AminoWritable writable) throws IOException,
            InterruptedException {
        // EMPTY
    }

}
