package com._42six.amino.common.bigtable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class BigTableRecordWriter extends RecordWriter<Text, Mutation>
{
	@SuppressWarnings("rawtypes")
	private RecordWriter wrappedRecordWriter;
	
	private BigTableDataWriter dataWriter;

	public BigTableRecordWriter(RecordWriter wrappedRecordWriter, BigTableDataWriter dataWriter) throws IOException
	{
		this.wrappedRecordWriter = wrappedRecordWriter;
		this.dataWriter = dataWriter;
		this.dataWriter.setRecordWriter(this.wrappedRecordWriter);
	}
	
	@Override
	public void write(Text table, Mutation m) throws IOException, InterruptedException
	{
		dataWriter.putNext(table, m);				
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException
	{
		wrappedRecordWriter.close(context);
	}
}
