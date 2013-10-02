package com._42six.amino.common;

import com.google.gson.Gson;

import java.io.IOException;
import java.util.Set;

/**
 * DataSources are the organizational unit for Features and Buckets.
 * @author Amino Team
 *
 */
public class DatasourceMetadata extends Metadata {

    public String description;
	public String name;
	public Set<String> featureIds;
	public Set<String> bucketIds;

    public DatasourceMetadata(){
        // EMPTY
    }

    public DatasourceMetadata(String desc, String id, String name, Set<String> bucketIds, Set<String> featureIds){
        this.description = desc;
        this.id = id;
        this.name = name;
	    this.bucketIds = bucketIds;
	    this.featureIds = featureIds;
    }

	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}
	
	public static DatasourceMetadata fromJson(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, DatasourceMetadata.class);
	}

    @Override
    /**
     * Combines two DatasourceMetadata objects together
     * @param with The DatasourceMetadata to combine with
     */
    public void combine(Metadata with) throws IOException {

        // Make sure with is of the right type
        if(with == null || with == this || !(with instanceof DatasourceMetadata)){
            return;
        }
        DatasourceMetadata that = (DatasourceMetadata) with;

        if(this.id.compareTo(that.id) != 0){
            throw new IOException("Can not combine buckets with different IDs.  Actual: " + that.id + " Expected: " + this.id);
        }

        this.featureIds.addAll(that.featureIds);
        this.bucketIds.addAll(that.bucketIds);

        // TODO Combine visibility labels ??

    }
	
}
