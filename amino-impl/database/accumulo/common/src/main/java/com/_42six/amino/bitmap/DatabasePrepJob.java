package com._42six.amino.bitmap;

import com._42six.amino.common.AminoConfiguration;
import com._42six.amino.common.Metadata;
import com._42six.amino.common.accumulo.*;
import com._42six.amino.common.bigtable.TableConstants;
import com._42six.amino.common.util.PathUtils;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.cli.Option;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Job for importing the metadata information from the framework driver into the Accumulo metadata table. Also creates
 * any tables that might be missing.  This is the first job run.
 */
public class DatabasePrepJob extends BitmapJob {

    private static boolean createTables(Configuration conf) throws IOException
    {
        final String instanceName = conf.get("bigtable.instance");
        final String zooKeepers = conf.get("bigtable.zookeepers");
        final String user = conf.get("bigtable.username");
        final String password = conf.get("bigtable.password");
        final String metaTable = conf.get(AminoConfiguration.TABLE_METADATA);
        final String hypoTable = conf.get(AminoConfiguration.TABLE_HYPOTHESIS);
        final String resultTable = conf.get(AminoConfiguration.TABLE_RESULT);
        final String membershipTable = conf.get(AminoConfiguration.TABLE_GROUP_MEMBERSHIP);
        final String groupHypothesisLUTable = conf.get(AminoConfiguration.TABLE_GROUP_HYPOTHESIS_LOOKUP);
        final String groupMetadataTable = conf.get(AminoConfiguration.TABLE_GROUP_METADATA);
        final String tableContext = conf.get(AminoConfiguration.TABLE_CONTEXT, "amino");
        final boolean blastMeta = conf.getBoolean(AminoConfiguration.FIRST_RUN, false);

        final TableOperations tableOps = IteratorUtils.connect(instanceName, zooKeepers, user, password).tableOperations();

        boolean success = IteratorUtils.createTable(tableOps, metaTable, tableContext, blastMeta, true);
        if (success) success = IteratorUtils.createTable(tableOps, hypoTable, tableContext, false, false);
        if (success) success = IteratorUtils.createTable(tableOps, resultTable, tableContext, false, false);
        if (success) success = IteratorUtils.createTable(tableOps, membershipTable, tableContext, false, false);
        if (success) success = IteratorUtils.createTable(tableOps, groupHypothesisLUTable, tableContext, false, false);
        if (success) success = IteratorUtils.createTable(tableOps, groupMetadataTable, tableContext, false, false);

        return success;
    }

    public static class MetadataConsolidatorReducer
            extends Reducer<Text, Text, Text, Mutation> {

        static final Gson gson = new Gson();
        static Text metadataTableText;

        @Override
        protected void setup(Context context){
            metadataTableText = new Text(context.getConfiguration().get(AminoConfiguration.TABLE_METADATA) + AminoConfiguration.TEMP_SUFFIX);
        }

        private <T extends Metadata & BtMetadata> void writeMutations(Class<T> cls, Iterable<Text> jsonValues, Context context)
                throws IOException, InterruptedException {
            T combinedMeta = null;
            for(Text value : jsonValues){
                final T meta = gson.fromJson(value.toString(), cls);
                if(meta == null){
                    throw new IOException("Could not serialize Metadata from JSON: " + value.toString());
                }

                // Set the combinedMeta if it's the first one
                if(combinedMeta == null) {
                    combinedMeta = meta;
                } else {
                    // The metadata is in sorted order.  Keep combining until we reach a different metadata type
                    if(combinedMeta.id.compareTo(meta.id) == 0){
                        combinedMeta.combine(meta);
                    } else {
                        // Found a new metadata type. Write the current combined metadata row out to the table
                        context.write(metadataTableText, combinedMeta.createMutation());
                        combinedMeta = meta;
                    }
                }
            }

            if(combinedMeta != null){
                final Mutation mutation = combinedMeta.createMutation();
                context.write(metadataTableText, mutation);
            }
        }

        /**
         * Takes all of the JSON objects of a particular type, combines them, and creates the Mutation for inserting
         * into the Accumulo table
         * @param metadataType The type of the metadata to combine
         * @param jsonValues The serialized JSON object to combine
         * @param context The MR context to write to Accumulo
         * @throws java.io.IOException
         * @throws InterruptedException
         */
        public void reduce(Text metadataType, Iterable<Text> jsonValues, Context context) throws IOException, InterruptedException
        {
            final String type = metadataType.toString();

            if(type.compareTo(TableConstants.BUCKET_PREFIX) == 0){
                writeMutations(BtBucketMetadata.class, jsonValues, context);
            } else if(type.compareTo(TableConstants.DATASOURCE_PREFIX) == 0){
                writeMutations(BtDatasourceMetadata.class, jsonValues, context);
            } else if (type.compareTo(TableConstants.DOMAIN_PREFIX) == 0){
                writeMutations(BtDomainMetadata.class, jsonValues, context);
            } else if(type.compareTo(TableConstants.FEATURE_PREFIX) == 0){
                writeMutations(BtFeatureMetadata.class, jsonValues, context);
            } else {
                throw new IOException("Unknown metadata type '" + type + ";");
            }
        }
    }

    public int run(String[] args) throws Exception {
        System.out.println("\n=============================== DatabasePrepJob ================================\n");

        // Create the command line options to be parsed
        final Option o1 = new Option("o", "outputDir", true, "The output directory");

        initializeConfigAndOptions(args, Optional.of(Sets.newHashSet(o1)));
        final Configuration conf = getConf();

        final Job job = new Job(conf, "Amino Metadata importer");
        job.setJarByClass(this.getClass());

        // Get config values
        final String instanceName = conf.get(TableConstants.CFG_INSTANCE);
        final String zooKeepers = conf.get(TableConstants.CFG_ZOOKEEPERS);
        final String user = conf.get(TableConstants.CFG_USER);
        final byte[] password = conf.get(TableConstants.CFG_PASSWORD).getBytes("UTF-8");
        final String metadataTable = conf.get(AminoConfiguration.TABLE_METADATA) + AminoConfiguration.TEMP_SUFFIX; //You want to make sure you use the temp here even if blastIndex is false
        final String metadataPaths = StringUtils.join(PathUtils.getMultipleJobMetadataPaths(conf,
                fromOptionOrConfig(Optional.of("o"), Optional.of(AminoConfiguration.OUTPUT_DIR))), ',');

        System.out.println("Metadata paths: [" + metadataPaths + "].");
        PathUtils.pathsExists(metadataPaths, conf);

        // TODO - Verify that all of the params above were not null

        job.setNumReduceTasks(1); // This needs to be 1

        // Mapper - use the IdentityMapper
        job.setMapOutputKeyClass(Text.class);

        // Reducer
        job.setReducerClass(MetadataConsolidatorReducer.class);

        // Inputs
        SequenceFileInputFormat.setInputPaths(job, metadataPaths);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // Outputs
        job.setOutputFormatClass(AccumuloOutputFormat.class);

        AccumuloOutputFormat.setZooKeeperInstance(job, new ClientConfiguration().withInstance(instanceName).withZkHosts(zooKeepers));
        AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(password));
        AccumuloOutputFormat.setCreateTables(job, true);
        AccumuloOutputFormat.setDefaultTableName(job, metadataTable);

        // Create the tables if they don't exist
        boolean complete = createTables(conf);
        if(complete){
            complete = job.waitForCompletion(true);
        }

        return complete ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DatabasePrepJob(), args));
    }
}
