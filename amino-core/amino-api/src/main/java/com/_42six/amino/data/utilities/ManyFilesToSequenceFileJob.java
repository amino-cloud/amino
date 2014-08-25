package com._42six.amino.data.utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class ManyFilesToSequenceFileJob extends Configured implements Tool  {

	/**
	 * @param args
	 * 0 - input directory full of files
	 * 1 - output SequenceFile
	 * 
	 * run like so: hadoop jar amino-data-0.0.1-SNAPSHOT.jar com._42six.amino.data.utilities.ManyFilesToSequenceFileJob /amino/rottenTomatoes/in /amino/rottenTomatoes/rotten-data
	 */
	public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ManyFilesToSequenceFileJob(), args));
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		try 
		{
			FileSystem fs = FileSystem.get(getConf());
			
			final Text key = new Text();
			final Text value = new Text();
			Writer writer = SequenceFile.createWriter(fs, getConf(), new Path(args[1]), key.getClass(), value.getClass(), SequenceFile.CompressionType.RECORD);
			
			System.out.println("in path: " + args[0]);
			final FileStatus[] stati = fs.listStatus(new Path(args[0]));
            for (FileStatus aStati : stati) {
                try(
                    FSDataInputStream in = fs.open(aStati.getPath());
                    InputStreamReader inR = new InputStreamReader(in);
                    BufferedReader buffer = new BufferedReader(inR)) {

                    long timestamp = System.currentTimeMillis();
                    String line;
                    while ((line = buffer.readLine()) != null) {
                        key.set(String.valueOf(timestamp));
                        value.set(line);
                        writer.append(key, value);
                    }
                }
            }
			
			writer.close();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		
		return 0;
	}

}
