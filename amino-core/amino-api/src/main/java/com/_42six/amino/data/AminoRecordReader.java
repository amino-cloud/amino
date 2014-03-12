package com._42six.amino.data;

import com._42six.amino.common.service.datacache.BucketCache;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class AminoRecordReader extends RecordReader<MapWritable, MapWritable> {

    public static final String DATA_SOURCE_NAME_KEY = "~dsn";

    private DataLoader dataLoader;

    @SuppressWarnings("rawtypes")
    private RecordReader recordReader = null;

    private MapWritable value;
    private MapWritable key;

    private Text dataSetName = null;

    public AminoRecordReader(InputSplit split, TaskAttemptContext context) throws InstantiationException {
        try {
            this.dataLoader = AminoDataUtils.getDataLoader(context.getConfiguration());
            this.initialize(split, context);
        } catch (Exception e) {
            throw new InstantiationException(e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        // close the underlying record reader
        if (this.recordReader != null) {
            this.recordReader.close();
        }
    }

    @Override
    public MapWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public MapWritable getCurrentValue() throws IOException,
            InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return this.recordReader.getProgress();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {

        if (this.dataLoader == null) {
            throw new IOException("DataLoader must be initialized first!!!!");
        }

        // initialize the underlying record reader
        if (this.recordReader == null) {
            //this.recordReader = this.inputFormat.createRecordReader(inputSplit, taskAttemptContext);
            this.recordReader = this.dataLoader.getInputFormat().createRecordReader(inputSplit, taskAttemptContext);
            this.recordReader.initialize(inputSplit, taskAttemptContext);
            this.dataLoader.setRecordReader(recordReader);
        }

        // read the available buckets from the bucket cache
        BucketCache bucketCache = new BucketCache(taskAttemptContext.getConfiguration());
        this.key = bucketCache.toMapWritableKey();
        this.key.put(new Text(DATA_SOURCE_NAME_KEY), new Text(this.dataLoader.getDataSourceName()));
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        value = this.dataLoader.getNext();
        if (value == null) {
            return false;
        }

        this.key = this.dataLoader.getNextKey(this.key); //This is new, most implementations should just return back the current key
        //if (dataSetName == null)
        //{//Don't do this in case they are actually checking something in the MapWritable to determine the datasetName
        dataSetName = new Text(this.dataLoader.getDataSetName(value));
        //}
        value.put(DataLoader.DATASET_NAME, dataSetName);
        return true;
    }

    public Text getDataSetName() {
        return dataSetName;
    }

    public Object getNextRaw() throws IOException {
        try {
            // We have nothing else so return null
            if (!this.recordReader.nextKeyValue()) {
                return null;
            }

            return this.recordReader.getCurrentValue();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

}
