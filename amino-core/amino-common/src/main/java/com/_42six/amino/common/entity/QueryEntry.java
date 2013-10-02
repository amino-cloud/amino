package com._42six.amino.common.entity;

import com.google.gson.Gson;

/**
 * A query entry represents one of the buckets found as the result of a query. It is captured as an object rather than
 * just a String so that more information can be added in later versions of Amino.
 *
 * @author Amino Team
 */
public class QueryEntry {

    public QueryEntry(){
        // EMPTY
    }

    public QueryEntry(String bucketName){
        this.bucketName = bucketName;
    }

	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}

	public static QueryEntry fromJson(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, QueryEntry.class);
	}

	public String bucketName;
}
