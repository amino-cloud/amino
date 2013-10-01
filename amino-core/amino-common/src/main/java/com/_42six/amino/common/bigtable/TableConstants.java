package com._42six.amino.common.bigtable;

import org.apache.hadoop.io.Text;

/**
 * Class for holding constants for the BigTable tables such as field names, etc
 */
public class TableConstants {

	// Column Family/Qualifiers that we re-use over and over
	public static final Text API_FIELD = new Text("api_version");
	public static final Text BUCKETID_FIELD = new Text("bucketIds");
	public static final Text DESCRIPTION_FIELD = new Text("description");
	public static final Text DATASOURCEIDS_FIELD = new Text("datasourceIds");
	public static final Text DISPLAYNAME_FIELD = new Text("displayName");
	public static final Text EMPTY_FIELD = new Text("");
	public static final Text FEATUREIDS_FIELD = new Text("featureIds");
	public static final Text JOB_FIELD = new Text("job_version");
	public static final Text JSON_FIELD = new Text("JSON");
	public static final Text NAME_FIELD = new Text("name");
	public static final Text NAMESPACE_FIELD = new Text("namespace");
	public static final Text TIMESTAMP_FIELD = new Text("timestamp");
	public static final Text TYPE_FIELD = new Text("type");
	public static final Text VISIBILITY_FIELD = new Text("visibility");

	public static final String ROW_DIVIDER = "#";
	public static final String ROW_TERMINATOR = "~~~~~~";

	// Prefixes for fetching an entity from the metadata table.  The entities are in the form entitiy#ID
	public static final String BUCKET_PREFIX = "bucket" + ROW_DIVIDER;
	public static final String DATASOURCE_PREFIX = "datasource" + ROW_DIVIDER;
	public static final String DOMAIN_PREFIX = "domain" + ROW_DIVIDER;
	public static final String FEATURE_PREFIX = "feature" + ROW_DIVIDER;

	// Text rows for terminating when a Range scan in the metadata table
	public static final Text BUCKET_END = new Text("bucket" + ROW_TERMINATOR);
	public static final Text DATASOURCE_END = new Text("datasource" + ROW_TERMINATOR);
	public static final Text DOMAIN_END = new Text("domain" + ROW_TERMINATOR);
	public static final Text FEATURE_END = new Text("feature" + ROW_TERMINATOR);

	// Ranges for grabbing an entity from the metadata table
//	public static final Range BUCKET_RANGE = new Range(new Text(BUCKET_PREFIX), BUCKET_END);
//	public static final Range DATASOURCE_RANGE = new Range(new Text(DATASOURCE_PREFIX), DATASOURCE_END);
//	public static final Range DOMAIN_RANGE = new Range(new Text(DOMAIN_PREFIX), DOMAIN_END);
//	public static final Range FEATURE_RANGE = new Range(new Text(FEATURE_PREFIX), FEATURE_END);

	public static final Text HASHCOUNT_FIELD = new Text("hashcount");
	public static final Text SHARDCOUNT_FIELD = new Text("shardcount");

    /** The prefix pre-pended to groups to signify that the String is a group and not an individual user */
    public static final String GROUP_PREFIX = "GROUP|";

    /** The prefix pre-pended to users to signify that the String is an user and not a group  */
    public static final String USER_PREFIX = "USER|";

    /** The group that everyone belongs to by default.  Used to share something with everyone */
    public static final String PUBLIC_GROUP = "GROUP|public";

}
