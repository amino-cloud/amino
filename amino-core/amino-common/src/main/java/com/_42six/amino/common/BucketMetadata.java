package com._42six.amino.common;

import com.google.gson.Gson;

import java.io.IOException;

/**
 * Metadata about buckets to be serialized/deserialized from Accumulo.
 */
public class BucketMetadata extends Metadata{
    /** Name of bucket */
    public String name;
    
    /** Display name of bucket */
    public String displayName;
    
    /** Visibility of bucket */
    public String visibility;
    
    /** Accumulo columnVisibility string */
    public String btVisibility;
    
    /** Name of the domain id */
    public String domainIdName;
    
    /** Description of the domain id */
    public String domainIdDescription;
    
    /** Timestamp in milliseconds of when the job ran */
    public long timestamp;
    
    public String toJson() {
        Gson gson = new Gson();
        
        return gson.toJson(this);
    }
    
    public static BucketMetadata fromJson(String json) {
        Gson gson = new Gson();
        
        return gson.fromJson(json, BucketMetadata.class);
    }
    
    public static BucketMetadata fromBucket(Bucket bucket) {
        BucketMetadata meta = new BucketMetadata();
        
        meta.name = bucket.getBucketName().toString();
        meta.displayName = bucket.getBucketDisplayName().toString();
        if (bucket.getDomainName() != null)
        {
        	meta.domainIdName = bucket.getDomainName().toString();
        }
        if (bucket.getDomainDescription() != null)
        {
        	meta.domainIdDescription = bucket.getDomainDescription().toString();
        }
        meta.timestamp = bucket.getTimestamp();
        
        return meta;
    }

    @Override
    /**
     * Combines two BucketMetadata objects together
     * @param with BucketMetadata to combine with
     */
    public void combine(Metadata with) throws IOException {

        // Make sure with is of the right type
        if(with == null || with == this || !(with instanceof BucketMetadata)){
            return;
        }
        BucketMetadata that = (BucketMetadata) with;

        if(this.id.compareTo(that.id) != 0){
            throw new IOException("Can not combine buckets with different IDs.  Actual: " + that.id + " Expected: " + this.id);
        }

        // Nothing to combine
        // TODO Combine visibility labels ??

    }
}
