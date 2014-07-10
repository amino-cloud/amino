package com._42six.amino.common;

import com.google.common.collect.Sets;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.*;

/**
 * Metadata about features to be serialized/deserialized from Accumulo.
 */
public class FeatureMetadata extends Metadata{

    /** Name of feature */
    public String name;
    
    /** Visibility of feature */
    public String visibility;
    
    /** The Accumulo columnVisibility string */
    public String btVisibility;
    
    /** API version */
    public String api_version;
    
    /** Job version */
    // TODO Probably not the best place to be hard coding a version string
    public String job_version = "0.2";
    
    /** Description of feature */
    public String description;
    
    /** Namespace */
    public String namespace;
    
    /** Feature type */
    public String type;

	/** The Datasources that the feature is associated with */
	public Set<String> datasources;

    /** Minimum value (for interval features) */
    public Hashtable<String,Double> min;
    
    /** Maximum value (for interval features) */
    public Hashtable<String,Double> max;
    
    /** Sorted set of allowed values (for nominal features) */
    public TreeSet<String> allowedValues;
    
    /** Total number of feature facts that were found in this feature */
    public Hashtable<String,Long> featureFactCount;
    
    /** Total number of bucket values that were found in this feature */
    public Hashtable<String,Long> bucketValueCount;
    
    /** Average ratio value for each bucket for this feature */
    public Hashtable<String,Double> averages;
    
    /** Standard deviation of the ratio values for each bucket for this feature */
    public Hashtable<String,Double> standardDeviations;
    
//    /** Percentiles of the ratio values for each bucket for this feature */
//    public Hashtable<String,Hashtable<Integer,Double>> percentiles;
    
    public Hashtable<String,ArrayList<Hashtable<String,Double>>> ratioBins;
    
    /** The top N nominal features for each bucket for this feature, stored in the json array like "featureFact:count" */
    public Hashtable<String,ArrayList<String>> topN;

    /**
     * Default constructor
     */
    public FeatureMetadata() { }

    /**
     * Copy constructor
     */
    public FeatureMetadata(FeatureMetadata that) {
        this.id = that.id;
        this.name = that.name;
        this.visibility = that.visibility;
        this.btVisibility = that.btVisibility;
        this.api_version = that.api_version;
        this.job_version = that.job_version;
        this.description = that.description;
        this.namespace = that.namespace;
        this.type = that.type;
        if (that.datasources != null) {
            this.datasources = new HashSet<String>(that.datasources);
        }
        if (that.min != null) {
            this.min = new Hashtable<String, Double>(that.min);
        }
        if (that.max != null) {
            this.max = new Hashtable<String, Double>(that.max);
        }
        if (that.allowedValues != null) {
            this.allowedValues = new TreeSet<String>(that.allowedValues);
        }
        if (that.featureFactCount != null) {
            this.featureFactCount = new Hashtable<String, Long>(that.featureFactCount);
        }
        if (that.bucketValueCount != null) {
            this.bucketValueCount = new Hashtable<String, Long>(that.bucketValueCount);
        }
        if (that.averages != null) {
            this.averages = new Hashtable<String, Double>(that.averages);
        }
        if (that.standardDeviations != null) {
            this.standardDeviations = new Hashtable<String, Double>(that.standardDeviations);
        }
        if (that.ratioBins != null) {
            this.ratioBins = new Hashtable<String, ArrayList<Hashtable<String, Double>>>(that.ratioBins);
        }
        if (that.topN != null) {
            this.topN = new Hashtable<String, ArrayList<String>>(that.topN);
        }
    }
    
    public void incrementFeatureFactCount(String bucketName)
    {
    	if (featureFactCount == null) featureFactCount = new Hashtable<String,Long>();
    	if (featureFactCount.containsKey(bucketName))
    	{
    		Long count = featureFactCount.get(bucketName);
    		count++;
    		featureFactCount.remove(bucketName);
    		featureFactCount.put(bucketName, count);
    	}
    	else
    	{
    		featureFactCount.put(bucketName, 1L);
    	}
    }
    
    public void addToBucketValueCount(String bucketName, Long count)
    {
    	if (bucketValueCount == null) {
    		bucketValueCount = new Hashtable<String,Long>();
    	}
    	
    	if (bucketValueCount.containsKey(bucketName))
    	{
    		Long val = bucketValueCount.get(bucketName);
    		val += count;
    		bucketValueCount.remove(bucketName);
    		bucketValueCount.put(bucketName, val);
    	}
    	else
    	{
    		bucketValueCount.put(bucketName, count);
    	}
    }
    
    public String toJson() {
        return new Gson().toJson(this);
    }
    
    /**
     * Factory method for deserializing FeatureMetadata and subclasses from json
     */
    public static FeatureMetadata fromJson(String json) {
        final FeatureMetadata meta = new Gson().fromJson(json, FeatureMetadata.class);
        return fromJson(json, (meta != null)? meta.type : null);
    }
    public static FeatureMetadata fromJson(String json, String type) {
        FeatureMetadata meta;
        final Gson gson = new Gson();

        if (FeatureFactType.dateIntervalTypes.contains(type)) {
            meta = gson.fromJson(json, DateFeatureMetadata.class);
        } else {
            meta = gson.fromJson(json, FeatureMetadata.class);
        }

        return meta;
    }
    
    public static FeatureMetadata fromFeature(Feature feature, FeatureFact fact, String datasourceId) {
        final FeatureMetadata meta = new FeatureMetadata();

        meta.id = String.valueOf(feature.hashCode());
        meta.name = feature.getName();
        meta.description = feature.getDescription();
        meta.namespace = feature.getNamespace();
        meta.type = fact.getType().name();
        meta.datasources = Sets.newHashSet(datasourceId);
        
        return meta;
    }

    @Override
    /**
     * Combines two FeatureMetadata objects together
     * @param that The object to combine with
     */
    public void combine(Metadata with) throws IOException {
        // Make sure with is of the right type
        if(with == null || with == this || !(with instanceof FeatureMetadata)){
            return;
        }

        FeatureMetadata that = (FeatureMetadata) with;

        // Verify that we can actually combine the metadatas together
        if(this.id.compareTo(that.id) != 0){
            throw new IOException("Can not combine features with different IDs.  Actual: " + that.id + " Expected: " + this.id);
        }

        if(this.job_version.compareTo(that.job_version) != 0){
            throw new IOException("Can not combine features with different job versions.  Actual: " +
                    that.job_version + " Expected: " + this.job_version);
        }

        if(this.api_version.compareTo(that.api_version) != 0){
            throw new IOException("Can not combine features with different API versions.  Actual: " +
                    that.api_version + " Expected: " + this.api_version);
        }

        if(this.namespace.compareTo(that.namespace) != 0){
            throw new IOException("Can not combine features with different namespace.  Actual: " +
                    that.namespace + " Expected: " + this.namespace);
        }

        if(this.type.compareTo(that.type) != 0){
            throw new IOException("Can not combine features with different type.  Actual: " +
                    that.type + " Expected: " + this.type);
        }

        // Combine appropriate fields

        if(that.datasources != null){
            if(this.datasources != null) {
                this.datasources.addAll(that.datasources);
            } else {
                this.datasources = new HashSet<String>(that.datasources);
            }
        }

        if(that.allowedValues != null){
            if(this.allowedValues != null) {
                this.allowedValues.addAll(that.allowedValues);
            } else {
                this.allowedValues = new TreeSet<String>(that.allowedValues);
            }
        }

        // TODO Combine visibility labels ??
    }

}
