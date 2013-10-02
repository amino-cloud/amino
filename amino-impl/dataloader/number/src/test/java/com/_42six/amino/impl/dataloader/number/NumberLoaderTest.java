package com._42six.amino.impl.dataloader.number;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.Bucket;
import com._42six.amino.data.AminoInputFormat;

public class NumberLoaderTest {
static class TestMapper extends Mapper<MapWritable, MapWritable, Bucket, MapWritable> {
		
		@Override
		protected void map(MapWritable keyIn, MapWritable valueIn, Context context) {
			Collection<Writable> valueSet = valueIn.values();
			for(Writable w : valueSet) {
				System.out.println(w);
			}
		
		}
		
	}
	
	public static void main(String [] args) throws IOException, InterruptedException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://10.1.10.31:54310");
		configuration.set("data.location", "/amino/playdata/numbers/numbers.txt");
		configuration.set(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY, "/amino/configs");
		
		Job job = new Job(configuration);
		job.setMapperClass(TestMapper.class);
		job.setNumReduceTasks(0);
		 
		
		NumberLoader kaggleLoader = new NumberLoader();
		AminoInputFormat.setDataLoader(job.getConfiguration(), kaggleLoader);
		
		AminoInputFormat inputFormat = new AminoInputFormat();
		List<InputSplit> splits = inputFormat.getSplits(job);
		
		TestMapper mapper = (TestMapper) job.getMapperClass().newInstance();
		for(InputSplit split : splits) {
			TaskAttemptID id = new TaskAttemptID();
			TaskAttemptContext attempt = new TaskAttemptContext(job.getConfiguration(), id);
			RecordReader<MapWritable, MapWritable> reader = inputFormat.createRecordReader(split, attempt);
			Mapper<MapWritable, MapWritable, Bucket, MapWritable>.Context context = mapper.new Context(job.getConfiguration(), id, reader, null, null, null, split);
			reader.initialize(split, context);
			mapper.run(context);
		}
	}
}
