#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )

package ${package};

import java.io.IOException;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.data.DataLoader;
import com._42six.amino.data.kaggle.KaggleLoader;

public class ${artifactId} implements DataLoader {
	
	@SuppressWarnings("rawtypes")
	private RecordReader reader;
	
	private static final Hashtable<Text, Text> bucketAndDisplayNames;
	
	static {
		bucketAndDisplayNames = new Hashtable<Text, Text>();
		// TODO: Add the bucket and display names to the hashmap 
	}
	
	@Override
	public InputFormat getInputFormat() {
		//TODO: Return the underlying InptuFormat
		return null;
	}

	@Override
	public void initializeFormat(Job job) throws IOException {
		// Load the default configuration for this class
		Configuration conf = job.getConfiguration();
		AminoConfiguration.loadDefault(conf, ${artifactId}.class.getSimpleName());

		//TODO: Add code to initialize the underlying format
		
	}

	@Override
	public MapWritable getNext() throws IOException {
		try {
			// We have nothing else so return null
			if (!reader.nextKeyValue()) {
				return null;
			}
			
			MapWritable retVal = new MapWritable();
			// TODO: Parse the data into the MapWritable
			return retVal;
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public List<Text> getBuckets() {
		return new LinkedList<Text>(bucketAndDisplayNames.keySet());
	}

	@Override
	public Hashtable<Text, Text> getBucketDisplayNames() {
		return bucketAndDisplayNames;
	}

	@Override
	public String getDataSourceName() {
		//TODO: Return the name of the datasource
		return null;
	}

	@Override
	public void setRecordReader(RecordReader recordReader) throws IOException {
		this.reader = recordReader;
	}
	
}
