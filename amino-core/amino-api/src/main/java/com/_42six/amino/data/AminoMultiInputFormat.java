package com._42six.amino.data;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;


public class AminoMultiInputFormat extends AminoInputFormat
{
	
	//public static void setJoinDataLoader(Configuration conf, DataLoader loader)
	public static void setJoinDataLoaders(Configuration conf, Iterable<Class<? extends DataLoader>> loaders)
			throws IOException {
		//AminoDataUtils.setJoinLoader(conf, loader);
		try {
			AminoDataUtils.setJoinDataLoaders(conf, loaders);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	
	public static void setEnrichWorker(Configuration conf, EnrichWorker ew)
			throws IOException {
		AminoDataUtils.setEnrichWorker(conf, ew);
	}
	
	//private boolean doLoaderTest(Configuration conf, InputSplit inputSplit, TaskAttemptContext context)
	private boolean doLoaderTest(DataLoader dl, Configuration conf, InputSplit inputSplit, TaskAttemptContext context)
	{
		AminoRecordReader test = null;
		try
		{
			//EnrichWorker ew = AminoDataUtils.getEnrichWorker(conf);
			//----Don't do this, won't work for Accumulo ---------------------------
			//test = new AminoRecordReader(inputSplit, context); 
			//Object row = test.getNextRaw();
			//-----------------------------------------------------------------------
	
			//DataLoader testDL = ew.determineDataLoaderFromRawLine(row).newInstance();
			//AminoDataUtils.setDataLoader(conf, testDL);
			if (dl.canReadFrom(inputSplit))
			{
				AminoDataUtils.setDataLoader(conf, dl);
				return true;
			}
			else
			{
				return false;
			}
			
			//return true;
		}
		catch (Exception ex)
		{
			//This shouldn't happen unless their canReadFrom breaks.  Want to print this out so it's seen...
			ex.printStackTrace();
			return false;
		}
		finally
		{
			if (test != null)
			{
				try 
				{
					test.close();
				} 
				catch (IOException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	private void setAppropriateLoader(Configuration conf, InputSplit inputSplit, TaskAttemptContext context) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException 
	{
		DataLoader dl = AminoDataUtils.getDataLoader(conf);
		
		Job myJob = new Job(new Configuration(conf));
		dl.initializeFormat(myJob); //gets the context stuff in the dataloader
		
		boolean result = doLoaderTest(dl, conf, inputSplit, context);
		if (!result)
		{
			Iterable<DataLoader> enrichers = AminoDataUtils.getJoinDataLoaders(conf);
			for (DataLoader edl : enrichers)
			{
				myJob = new Job(new Configuration(conf));
				edl.initializeFormat(myJob); //gets the context stuff in the dataloader
				
				//AminoDataUtils.setDataLoader(conf, edl);
				
				//if (doLoaderTest(edl, conf, inputSplit, context)) break;
				result = doLoaderTest(edl, conf, inputSplit, context);
				if (result) break;
			}
		}
		
		if (!result)
		{
			throw new IOException("No dataloaders matched for the input paths provided, please ensure you've implemented canReadFrom(InputSplit inputSplit) properly in all your DataLoaders used in this multi input job.");
		}
	}
	
	//This doesn't work because the getInputFormatClass is returning the parent AminoMultiInputFormat and not the more specific one
	//This ends up pushing us through a stackOverflow because AminoMultiInputFormat keeps calling createRecordReader, which in turn keeps calling this method
//	private void setAppropriateLoader(Configuration conf, InputSplit inputSplit, TaskAttemptContext context) throws InstantiationException, IllegalAccessException, IOException, InterruptedException, ClassNotFoundException
//	{
//		EnrichWorker ew = AminoDataUtils.getEnrichWorker(conf);
//		AminoRecordReader test = new AminoRecordReader(context.getInputFormatClass().newInstance(), inputSplit, context); 
//		Object row = test.getNextRaw();
//		test.close();
//
//		System.err.println(row.toString());
//		DataLoader testDL = ew.determineDataLoaderFromRawLine(row).newInstance();
//		System.err.println("data loader: " + testDL.getClass().getSimpleName());
//		AminoDataUtils.setDataLoader(conf, testDL);
//	}
	
	//This is the old way with one enricher...
//	private void setAppropriateLoader(Configuration conf, InputSplit inputSplit, TaskAttemptContext context) throws ClassNotFoundException, InstantiationException, IllegalAccessException
//	{
//		DataLoader dl = AminoDataUtils.getDataLoader(conf);
//		AminoRecordReader test = null;
//		try
//		{
//			Job myJob = new Job(conf);
//			dl.initializeFormat(myJob); //gets the context stuff in the dataloader
//			HadoopConfigurationUtils.mergeConfs(conf, myJob.getConfiguration());
//			
//			EnrichWorker ew = AminoDataUtils.getEnrichWorker(conf);
//			test = new AminoRecordReader(dl.getInputFormat(), inputSplit, context); //instantiates the dataloader from the context
//			Object row = test.getNextRaw();
//	
//			DataLoader testDL = ew.determineDataLoaderFromRawLine(row).newInstance();
//			if (!testDL.getDataSourceName().equals(dl.getDataSourceName()))
//			{
//				AminoDataUtils.setLoader(conf, AminoDataUtils.getJoinDataLoader(conf));
//			}
//		} 
//		catch (Exception ex)
//		{
//			//This will happen if the input formats are different, oh well, no way around this...
//			AminoDataUtils.setLoader(conf, AminoDataUtils.getJoinDataLoader(conf));
//		}
//		finally
//		{
//			if (test != null)
//			{
//				try 
//				{
//					test.close();
//				} 
//				catch (IOException e) 
//				{
//					e.printStackTrace();
//				}
//			}
//		}
//	}
	
	@Override
	public RecordReader<MapWritable, MapWritable> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException 
	{
		try
		{
			Configuration conf = context.getConfiguration();
			setAppropriateLoader(conf, inputSplit, context);
			return super.createRecordReader(inputSplit, context);
			
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
	public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException 
	{
		try {
			DataLoader loader = AminoDataUtils.getDataLoader(jobContext.getConfiguration());
//			joinLoader = AminoDataUtils.getJoinDataLoader(jobContext.getConfiguration());
			
			Configuration conf = jobContext.getConfiguration();
			Job myJob = new Job(new Configuration(conf));
			loader.initializeFormat(myJob);

//			List<InputSplit> retVal = loader.getInputFormat().getSplits(new JobContextImpl(myJob.getConfiguration(), myJob.getJobID()));
            List<InputSplit> retVal = loader.getInputFormat().getSplits(jobContext);

//			myJob = new Job(jobContext.getConfiguration());
//			//Need this because loadDefault doesn't override properties from the previous data loader, so if both loaders use the same property for the data location, it will use the old location
//			AminoConfiguration.overrideDefault(jobContext.getConfiguration(), joinLoader.getClass().getSimpleName());
//			joinLoader.initializeFormat(myJob);
//			retVal.addAll(joinLoader.getInputFormat().getSplits(new JobContext(myJob.getConfiguration(), myJob.getJobID())));
			
			Iterable<DataLoader> erichers = AminoDataUtils.getJoinDataLoaders(conf);
			for (DataLoader edl : erichers)
			{
				retVal = addInputToContext(retVal, edl, jobContext);
			}
			
			return retVal;
			
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		} catch (InstantiationException e) {
			throw new IOException(e);
		} catch (IllegalAccessException e) {
			throw new IOException(e);
		}
	}
	
	@SuppressWarnings("unchecked")
	private List<InputSplit> addInputToContext(List<InputSplit> retVal, DataLoader joinLoader, JobContext jobContext) throws IOException, InterruptedException
	{
		Job myJob = new Job(new Configuration(jobContext.getConfiguration()));

		//No need for this...happens in initializeFormat
		//AminoConfiguration.loadDefault(myJob.getConfiguration(), joinLoader.getClass().getSimpleName());
		joinLoader.initializeFormat(myJob);
//		retVal.addAll(joinLoader.getInputFormat().getSplits(new JobContextImpl(myJob.getConfiguration(), myJob.getJobID())));
		retVal.addAll(joinLoader.getInputFormat().getSplits(jobContext));

		return retVal;
	}

}
