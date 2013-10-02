package com._42six.amino.common.bigtable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;

import org.apache.hadoop.mapreduce.Job;

public interface BigTableDataWriter
{
	public void initializeFormat(Job job) throws IOException;
	
	@SuppressWarnings("rawtypes")
	public OutputFormat getOutputFormat() throws IOException;

	@SuppressWarnings("rawtypes")
	public void setRecordWriter(RecordWriter writer) throws IOException;

	public void putNext(Text table, Mutation m) throws IOException;
	
	public void setConfig(Configuration conf);
}
