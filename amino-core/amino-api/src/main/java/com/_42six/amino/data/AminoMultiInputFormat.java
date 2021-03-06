package com._42six.amino.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;


public class AminoMultiInputFormat extends AminoInputFormat
{
	public static void setJoinDataLoaders(Configuration conf, Iterable<Class<? extends DataLoader>> loaders)
			throws IOException
    {
		try {
			AminoDataUtils.setJoinDataLoaders(conf, loaders);
		} catch (InstantiationException  | IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	
	public static void setEnrichWorker(Configuration conf, EnrichWorker ew){
		AminoDataUtils.setEnrichWorker(conf, ew);
	}

	private boolean doLoaderTest(DataLoader dl, Configuration conf, InputSplit inputSplit, TaskAttemptContext context)
	{
		try
		{
			//EnrichWorker ew = AminoDataUtils.getEnrichWorker(conf);
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
		}
		catch (Exception ex)
		{
			// This shouldn't happen unless their canReadFrom breaks.  Want to print this out so it's seen...
			ex.printStackTrace();
			return false;
		}
	}
	
	private void setAppropriateLoader(Configuration conf, InputSplit inputSplit, TaskAttemptContext context) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException 
	{
		DataLoader dl = AminoDataUtils.createDataLoader(conf);
		
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
//		DataLoader dl = AminoDataUtils.createDataLoader(conf);
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
			setAppropriateLoader(context.getConfiguration(), inputSplit, context);
		} catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
            throw new IOException(e);
        }
		return super.createRecordReader(inputSplit, context);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException 
	{
        List<InputSplit> retVal;
		try {
			final DataLoader loader = AminoDataUtils.createDataLoader(jobContext.getConfiguration());
			final Configuration conf = jobContext.getConfiguration();
			final Job myJob = new Job(new Configuration(conf));
			loader.initializeFormat(myJob);

			retVal = loader.getInputFormat().getSplits(myJob);

			final Iterable<DataLoader> enrichers = AminoDataUtils.getJoinDataLoaders(conf);
			for (DataLoader enrichDataLoader : enrichers)
			{
				retVal = addInputToContext(retVal, enrichDataLoader, jobContext);
			}
		} catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
			throw new IOException(e);
		}
		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	private List<InputSplit> addInputToContext(List<InputSplit> retVal, DataLoader joinLoader, JobContext jobContext) throws IOException, InterruptedException
	{
		final Job myJob = new Job(new Configuration(jobContext.getConfiguration()));
		joinLoader.initializeFormat(myJob);
		retVal.addAll(joinLoader.getInputFormat().getSplits(myJob));

		return retVal;
	}

}
