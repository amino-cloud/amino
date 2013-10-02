package com._42six.amino.data;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com._42six.amino.common.HadoopConfigurationUtils;

public class AminoInputFormat extends InputFormat<MapWritable, MapWritable> {

	public static void setDataLoader(Configuration conf, DataLoader loader)
			throws IOException {
		AminoDataUtils.setDataLoader(conf, loader);
	}

	@Override
	public RecordReader<MapWritable, MapWritable> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		try {
			Configuration conf = context.getConfiguration();
			DataLoader dl = AminoDataUtils.getDataLoader(conf);
			Job myJob = new Job(conf);
			dl.initializeFormat(myJob);
			/* Since we create a new job to initalize the input format, and the
			   constructor of the Job class does a deep copy of the
			   configuration, the initalized format configuration never makes it
			   back into our original context configuration to be passed on. In
			   order to fix this, once we get back the configuration from the
			   underlying input format, we merge that back into the
			   TaskAttemptContext's configuration, so that our values are set.*/
			HadoopConfigurationUtils.mergeConfs(conf, myJob.getConfiguration());
			//return new AminoRecordReader(dl.getInputFormat(), inputSplit, context);
			return new AminoRecordReader(inputSplit, context);
		} catch (InstantiationException e) {
			throw new IOException(e);
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		} catch (IllegalAccessException e) {
			throw new IOException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<InputSplit> getSplits(JobContext jobContext)
			throws IOException, InterruptedException {
		DataLoader loader;
		try {
			loader = AminoDataUtils.getDataLoader(jobContext
					.getConfiguration());
			Job myJob = new Job(jobContext.getConfiguration());
			loader.initializeFormat(myJob);
			@SuppressWarnings("rawtypes")
			InputFormat inputFormat = loader.getInputFormat();
			List<InputSplit> retVal = inputFormat.getSplits(new JobContext(
					myJob.getConfiguration(), myJob.getJobID()));
			return retVal;
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		} catch (InstantiationException e) {
			throw new IOException(e);
		} catch (IllegalAccessException e) {
			throw new IOException(e);
		}
	}

}
