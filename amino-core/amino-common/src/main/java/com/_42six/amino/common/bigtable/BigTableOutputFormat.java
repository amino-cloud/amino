package com._42six.amino.common.bigtable;

import com._42six.amino.common.HadoopConfigurationUtils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class BigTableOutputFormat extends OutputFormat<Text, Mutation>
{
	private final static String BIGTABLE_DATA_WRITER_CONFIGURATION_KEY = "bigtable.datawriter";

	public static void setBigTableDataWriter(Configuration configuration, BigTableDataWriter writer)
	{
		configuration.setClass(BIGTABLE_DATA_WRITER_CONFIGURATION_KEY, writer.getClass(), BigTableDataWriter.class);
	}

	
	public static BigTableDataWriter getBigTableDataWriter(Configuration conf) throws ClassNotFoundException,InstantiationException,IllegalAccessException
	{
		Class<? extends BigTableDataWriter> tmp = conf.getClass(BIGTABLE_DATA_WRITER_CONFIGURATION_KEY, null, BigTableDataWriter.class);
		if(tmp == null)
		{
			throw new ClassNotFoundException("Could not find Bigtable data write, please set the bigtable data writer!");
		}
		BigTableDataWriter retVal = tmp.newInstance();
		retVal.setConfig(conf);
		return retVal;
	}	

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException
	{
		try 
		{
			Configuration conf = context.getConfiguration();
			BigTableDataWriter writer = getBigTableDataWriter(conf);
			Job myJob = new Job(conf);
			writer.initializeFormat(myJob);
			
			// We need to merge our configurations together
			HadoopConfigurationUtils.mergeConfs(conf, myJob.getConfiguration());

			writer.getOutputFormat().checkOutputSpecs(context);
		} catch(InstantiationException e) {
			throw new IOException(e);	
		} catch(ClassNotFoundException e) {
			throw new IOException(e);	
		} catch(IllegalAccessException e) {
			throw new IOException(e);	
		}	
	}
	
	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException
	{
		try 
		{
			BigTableDataWriter writer = getBigTableDataWriter(context.getConfiguration());
			return writer.getOutputFormat().getOutputCommitter(context);
		} catch(InstantiationException e) {
			throw new IOException(e);	
		} catch(ClassNotFoundException e) {
			throw new IOException(e);	
		} catch(IllegalAccessException e) {
			throw new IOException(e);	
		}
	}
	
	@Override
	public RecordWriter<Text, Mutation> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException
	{
		try 
		{
			Configuration conf = context.getConfiguration();
			BigTableDataWriter writer = getBigTableDataWriter(conf);
			//Job myJob = new Job(conf);
			// writer.initializeFormat(myJob);
			
			// We need to merge our configurations together
			//HadoopConfigurationUtils.mergeConfs(conf, myJob.getConfiguration());

			@SuppressWarnings("rawtypes")
			RecordWriter wrappedWriter = writer.getOutputFormat().getRecordWriter(context);
			return new BigTableRecordWriter(wrappedWriter, writer);
		} catch(InstantiationException e) {
			throw new IOException(e);	
		} catch(ClassNotFoundException e) {
			throw new IOException(e);	
		} catch(IllegalAccessException e) {
			throw new IOException(e);	
		}
	}
}
