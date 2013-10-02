package com._42six.amino.common.entity;

import com.google.gson.Gson;

import java.util.List;

/**
 * A QueryResult represents, as you might have guessed by its name, the results of a query.
 * When executed as part of a {@link com._42six.amino.query.services.AminoQueryService#testResult}, only its result_count will be set.
 *
 */
public class QueryResult {
	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}

	public static QueryResult fromJson(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, QueryResult.class);
	}

	public String owner;
	public String id;
	public Long timestamp;
	public Long result_count;
	public String hypothesisid;
	public String hypothesisname;
	public String bucketid;
	public Hypothesis hypothesis_at_runtime;
	public String error;
	public List<QueryEntry> result_set;
}
