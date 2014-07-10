package com._42six.amino.data;

import com._42six.amino.common.*;
import com._42six.amino.common.bigtable.TableConstants;
import com._42six.amino.common.index.BitmapIndex;
import com._42six.amino.common.util.Environment;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;

public class MetadataWriter {

    private static final Logger logger = LoggerFactory.getLogger(MetadataWriter.class);

    private final Configuration conf;
    private final String version;
    
    private class BucketHolder {
        final public String source;
        final public BucketMetadata metadata;
        final public String visibility;
        
        public BucketHolder(String source, BucketMetadata metadata, String visibility, String humanReadableVisibility) {
            this.source = source;
            this.metadata = metadata;
            this.metadata.btVisibility = visibility;
            this.metadata.visibility = humanReadableVisibility;
            this.visibility = visibility;
        }
    }

    private class FeatureHolder {
        final public String source;
        final public FeatureMetadata metadata;
        final public String visibility;
        
        public FeatureHolder(String source, FeatureMetadata metadata, String visibility, String humanReadableVisibility) {
            this.source = source;
            this.metadata = metadata;
            this.metadata.btVisibility = visibility;
            this.metadata.visibility = humanReadableVisibility;
            this.metadata.api_version = version;
            // TODO job version....once data gets moved into api and we don't have the circular dependency problem
            this.visibility = visibility;
        }
    }

    private final Map<String, FeatureHolder> featureList = new HashMap<String, FeatureHolder>();
    private final Map<String, BucketHolder> bucketList = new HashMap<String, BucketHolder>();
	private final Map<String, DatasourceMetadata> datasources = new HashMap<String, DatasourceMetadata>();

    public MetadataWriter(Configuration conf) {
        this.conf = conf;
        this.version = Environment.getImplementationVersion();
    }

    private final static Text FEATURE_TEXT = new Text(TableConstants.FEATURE_PREFIX);
    private final static Text DATASOURCE_TEXT = new Text(TableConstants.DATASOURCE_PREFIX);
    private final static Text DOMAIN_TEXT = new Text(TableConstants.DOMAIN_PREFIX);
    private final static Text BUCKET_TEXT = new Text(TableConstants.BUCKET_PREFIX);

    /**
     * Writes all of the data for the amino_metadata table to HDFS for reading in a separate job
     * @throws IOException
     */
    public void write() throws IOException {
        final String outputDir = conf.get(AminoConfiguration.OUTPUT_DIR) + "/cache/metadata";
        final FileSystem fs = FileSystem.get(URI.create(outputDir), conf);
        final Path seqFile = new Path(outputDir + "/" + UUID.randomUUID().toString() + ".seq");
        SequenceFile.Writer writer = null;

        if (!featureList.isEmpty()) {
            try {
                writer = SequenceFile.createWriter(fs, conf, seqFile, Text.class, Text.class);
                logger.info("Writing out metadata seq file: " + seqFile.getName());

                // Add features
                for (FeatureHolder holder : featureList.values()) {
                    writer.append(FEATURE_TEXT, new Text(holder.metadata.toJson()));
                }

                // Create the special RESTRICT feature
                final FeatureHolder fh = (FeatureHolder) featureList.values().toArray()[0];
                final FeatureMetadata fm = new FeatureMetadata();
                fm.api_version = fh.metadata.api_version;
                fm.btVisibility = fh.metadata.btVisibility;
                fm.visibility = fh.metadata.visibility;
                fm.id = "1";
                fm.name = "Restrict";
                fm.type = "RESTRICTION";
                fm.description = "Restrict value to";
                fm.namespace = "Public";
                fm.datasources = Sets.newHashSet(fh.source); // TODO - Investigate if this is proper
                writer.append(FEATURE_TEXT, new Text(fm.toJson()));

                // TODO figure out the proper way to populate the domain metadata vs datasource metadata
                int domainID = 0;
                for(DatasourceMetadata datasource : datasources.values()){
                    domainID += datasource.id.hashCode();
                }

                DomainMetadata domainMetadata = new DomainMetadata(TableConstants.DOMAIN_PREFIX + domainID,
                        null, null, new HashSet<String>(datasources.size()));

                // Add Datasources
                // TODO figure out what the column visibilities should be for the datasources
                for(DatasourceMetadata source : datasources.values()){
                    domainMetadata.datasources.add(source.id);
                    domainMetadata.name = source.name;
                    domainMetadata.description = source.description;

                    writer.append(DATASOURCE_TEXT, new Text(source.toJson()));
                }

                // Add Domain
                writer.append(DOMAIN_TEXT, new Text(domainMetadata.toJson()));

                // Add buckets
                for (BucketHolder holder : bucketList.values()) {
                    final BucketMetadata meta = holder.metadata;
                    meta.id = new Text(Integer.toString(BitmapIndex.getBucketNameIndex(holder.source, meta.name))).toString();
                    writer.append(BUCKET_TEXT, new Text(meta.toJson()));
                }
            } finally {
                if(writer != null){
                    IOUtils.closeStream(writer);
                }
            }
        }
    }

    public void addToMetadata(Bucket bucket, AminoWritable result) {
        final Feature feature = result.getFeature();
        final FeatureFact fact = result.getFeatureFact();
        final String source = bucket.getBucketDataSource().toString();
        final String visibility = bucket.getBucketVisibility().toString();
        final String hrVisibility = bucket.getBucketHRVisibility().toString();
	    final String bucketName = bucket.getBucketName().toString();
	    final String bucketId = Integer.toString(BitmapIndex.getBucketNameIndex(source, bucketName));
	    final String datasourceId = bucket.getBucketDataSource().toString();

        bucketList.put(bucketName,
                new BucketHolder(source, BucketMetadata.fromBucket(bucket), visibility, hrVisibility));

	    FeatureMetadata featureMeta = FeatureMetadata.fromFeature(feature, fact, datasourceId);
        featureList.put(feature.getName() + feature.getNamespace(),
                new FeatureHolder(source, featureMeta, visibility, hrVisibility));

	    // check if the datasource already exists and if so update the featureIds and bucketIds
	    if(datasources.containsKey(datasourceId)){
		    final DatasourceMetadata datasourceMetadata = datasources.get(datasourceId);
            datasourceMetadata.featureIds.add(featureMeta.id);
            datasourceMetadata.bucketIds.add(bucketId);
	    } else {
		    final Set<String> bucketIds = new HashSet<String>();
		    final Set<String> featureIds = new HashSet<String>();
		    bucketIds.add(bucketId);
		    featureIds.add(featureMeta.id);

		    final DatasourceMetadata ds = new DatasourceMetadata(bucket.getDomainDescription().toString(),
				    bucket.getBucketDataSource().toString(), bucket.getDomainName().toString(),
				    bucketIds, featureIds);
		    datasources.put(datasourceId, ds);
	    }
    }
}
