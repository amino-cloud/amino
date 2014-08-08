package com._42six.amino.data;

import com._42six.amino.common.HadoopConfigurationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class AminoInputFormat extends InputFormat<MapWritable, MapWritable> {

    private static final Logger logger = LoggerFactory.getLogger(AminoInputFormat.class);

    public static void setDataLoader(Configuration conf, DataLoader loader)
            throws IOException {
        logger.info("Setting DataLoader to: {}", loader.getClass().getCanonicalName());
        AminoDataUtils.setDataLoader(conf, loader);
    }

    @Override
    public RecordReader<MapWritable, MapWritable> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        try {
            final Configuration conf = context.getConfiguration();
            final DataLoader dl = AminoDataUtils.createDataLoader(conf);
            final Job myJob = new Job(conf);

            dl.initializeFormat(myJob);

            /* Since we create a new job to initialize the input format, and the
			   constructor of the Job class does a deep copy of the
			   configuration, the initialized format configuration never makes it
			   back into our original context configuration to be passed on. In
			   order to fix this, once we get back the configuration from the
			   underlying input format, we merge that back into the
			   TaskAttemptContext's configuration, so that our values are set.*/
            HadoopConfigurationUtils.mergeConfs(conf, myJob.getConfiguration());

            return new AminoRecordReader(inputSplit, context);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new IOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<InputSplit> getSplits(JobContext jobContext)
            throws IOException, InterruptedException {
        try {
            final Configuration conf = jobContext.getConfiguration();
            final DataLoader loader = AminoDataUtils.createDataLoader(conf);
            final Job myJob = new Job(conf);

            loader.initializeFormat(myJob);
            @SuppressWarnings("rawtypes")
            InputFormat inputFormat = loader.getInputFormat();

            return inputFormat.getSplits(myJob);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new IOException(e);
        }
    }

}
