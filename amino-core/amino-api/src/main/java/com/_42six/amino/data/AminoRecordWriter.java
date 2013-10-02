package com._42six.amino.data;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Bucket;

public class AminoRecordWriter extends RecordWriter<Bucket, AminoWritable>
{

	@Override
	public void close(TaskAttemptContext arg0) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(Bucket arg0, AminoWritable arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
	}

}
