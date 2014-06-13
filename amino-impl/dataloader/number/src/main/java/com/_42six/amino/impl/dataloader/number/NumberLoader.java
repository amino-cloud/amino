package com._42six.amino.impl.dataloader.number;

import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.data.DataLoader;
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

import java.io.IOException;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

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

    @Override
    public InputFormat getInputFormat() {
        return new TextInputFormat();
    }

    @Override
    public void initializeFormat(Job job) throws IOException {
        final Configuration conf = job.getConfiguration();

        AminoConfiguration.loadAndMerge(conf, NumberLoader.class.getSimpleName(), true);
        dataLoc = conf.get("data.location");
        FileInputFormat.setInputPaths(job, dataLoc);
        number2Max = conf.getInt("number2-max", 500);
    }

    @Override
    public MapWritable getNext() throws IOException {
        Text val;
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
            if (twoVal <= number2Max) {
                retVal.put(new Text("number2"), new Text(val.toString()));
            }
            return retVal;
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<Text> getBuckets() {
        return new LinkedList<Text>(NumberLoader.bucketsAndDisplayNames.keySet());
    }

    @Override
    public Hashtable<Text, Text> getBucketDisplayNames() {
        return NumberLoader.bucketsAndDisplayNames;
    }

    @Override
    public String getDataSourceName() {
        return "numbers";
    }

    @Override
    public void setRecordReader(RecordReader recordReader) throws IOException {
        this.reader = recordReader;
    }

    @Override
    public void setConfig(Configuration config) {
        this.config = config;
    }

    @Override
    public String getVisibility() {
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
    public boolean canReadFrom(InputSplit inputSplit) {
        FileSplit fs = (FileSplit) inputSplit;
        if (fs.getPath().toString().contains(dataLoc)) {
            System.out.println("true");
            System.out.println(fs.getPath().toString() + " --- " + dataLoc);
            return true;
        } else {
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
