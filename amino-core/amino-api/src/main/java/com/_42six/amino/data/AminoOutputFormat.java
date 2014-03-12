package com._42six.amino.data;

import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Bucket;
import com._42six.amino.common.BucketStripped;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class AminoOutputFormat extends SequenceFileOutputFormat<Bucket, AminoWritable> {

    public static void setAminoConfigPath(Job job, String hdfsAminoConfigPath) {
        job.getConfiguration().set(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY, hdfsAminoConfigPath);
    }

    protected SequenceFile.Writer getSequenceWriter(TaskAttemptContext context, Class<?> keyClass, Class<?> valueClass) throws IOException {

        Configuration conf = context.getConfiguration();
        CompressionCodec codec = null;
        CompressionType compressionType = CompressionType.NONE;
        if (getCompressOutput(context)) {
            // find the kind of compression to do
            compressionType = getOutputCompressionType(context);
            // find the right codec
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context,
                    DefaultCodec.class);
            codec = (CompressionCodec)
                    ReflectionUtils.newInstance(codecClass, conf);
        }
        // get the path of the temporary output file
        Path file = getDefaultWorkFile(context, "");
        FileSystem fs = file.getFileSystem(conf);
        return SequenceFile.createWriter(fs, conf, file,
                keyClass,
                valueClass,
                compressionType,
                codec,
                context);
    }

    @Override
    public RecordWriter<Bucket, AminoWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        final SequenceFile.Writer out = getSequenceWriter(context, context.getOutputKeyClass(), context.getOutputValueClass());
        final MetadataWriter metawriter = new MetadataWriter(context.getConfiguration());

        return new AminoRecordWriter() {

            public void write(Bucket key, AminoWritable value) throws IOException {
                // Add to metadata in memory
                metawriter.addToMetadata(key, value);

                // Only write out the stripped bucket
                BucketStripped stripped = BucketStripped.fromBucket(key);
                out.append(stripped, value);
            }

            public void close(TaskAttemptContext context) throws IOException {
                //Finally, write the metadata
                metawriter.write();
                out.close();
            }
        };
    }
}
