package com._42six.amino.impl.dataloader.number;

import java.io.IOException;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.data.DataLoader;

public class NumberLoader implements DataLoader {
	
	@SuppressWarnings("rawtypes")
	private RecordReader reader;
	private Configuration config;
	private int number2Max = 500;
	private String dataLoc = "";

	private static final Hashtable<Text, Text> bucketsAndDisplayNames;
	
	static {
		bucketsAndDisplayNames = new Hashtable<Text, Text>();
		bucketsAndDisplayNames.put(new Text("number"), new Text("number"));
		bucketsAndDisplayNames.put(new Text("number2"), new Text("number2"));
	}
	
	public InputFormat getInputFormat() {
		// TODO Auto-generated method stub
		return new TextInputFormat();
	}

	public void initializeFormat(Job job) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = job.getConfiguration();
		AminoConfiguration.loadDefault(conf, NumberLoader.class.getSimpleName(), true);
		dataLoc = conf.get("data.location");
		FileInputFormat.setInputPaths(job,
				dataLoc);
		number2Max = conf.getInt("number2-max", 500);
	}

	public MapWritable getNext() throws IOException {
		// TODO Auto-generated method stub
		Text val = null;
		try {
			// We have nothing else so return null
			if (!this.reader.nextKeyValue()) {
				return null;
			}
			MapWritable retVal = new MapWritable();
			// Parse our data
			val = (Text) reader.getCurrentValue();
			retVal.put(new Text("number"), new Text(val.toString()));
			int twoVal = Integer.parseInt(val.toString());
			if (twoVal <= number2Max)
			{
				retVal.put(new Text("number2"), new Text(val.toString()));
			}
			return retVal;
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	public List<Text> getBuckets() {
		// TODO Auto-generated method stub
		return new LinkedList<Text>(NumberLoader.bucketsAndDisplayNames.keySet());
	}

	public Hashtable<Text, Text> getBucketDisplayNames() {
		// TODO Auto-generated method stub
		return NumberLoader.bucketsAndDisplayNames;
	}

	public String getDataSourceName() {
		// TODO Auto-generated method stub
		return "numbers";
	}

	public void setRecordReader(RecordReader recordReader) throws IOException {
		this.reader = recordReader;
	}

	public void setConfig(Configuration config) {
		this.config = config;
	}

	@Override
	public String getVisibility() 
	{
		return "U";
	}

	@Override
	public String getDataSetName(MapWritable mw) {
		return "numbersDS";
	}

	@Override
	public String getHumanReadableVisibility() {
		return "UNCLASSIFIED";
	}

	@Override
	public boolean canReadFrom(InputSplit inputSplit) 
	{
		FileSplit fs = (FileSplit)inputSplit;
		if (fs.getPath().toString().contains(dataLoc))
		{
			System.out.println("true");
			System.out.println(fs.getPath().toString() + " --- " + dataLoc);
			return true;
		}
		else
		{
			System.out.println("false");
			System.out.println(fs.getPath().toString() + " --- " + dataLoc);
			return false;
		}
	}

	@Override
	public MapWritable getNextKey(MapWritable currentKey) throws IOException,
			InterruptedException {
		return currentKey;
	}
}
