/*
 * This generates the Thrift service for the Amino QueryService.  You must have Thrift 0.6.1 installed.
 *
 * Run the generate.sh script to generate all of the Thrift classes and place them in the appropriate spot in the src tree
 */

namespace java com._42six.amino.query.thrift.services

include "Common.thrift"

service ThriftQueryService {
	/**
	 * Lists existing AminoQueryResults for a given user.
	 *
	 * @param start     There are two parameters, both optional, used for paging. start defaults to 0 if not specified, and count defaults to 20 if not specified.
     * @param count     How many to return.
	 * @param userid     The userid to search on.
	 * @param visibilities A list of string corresponding to allowed visibilities for the user.
	 * @return A list of AminoQueryResults.
	 */
	list<Common.TQueryResult> listResults(1:i64 start, 2: i64 count, 3: string userid, 4: set<string> visibilities);

	/**
	 * Get an already existing result from the data store.
	 *
	 * @param requester  The ID String of the person making the request
	 * @param owner      The owner string of the QueryResult
	 * @param id         The id of the result.
	 * @param visibilities A list of string corresponding to allowed visibilities for the user.
	 * @return The filled out AminoQueryResults.
	 */
	Common.TQueryResult getResult(1: string requester, 2: string owner, 3: string id, 4: set<string> visibilities);

	/**
	 * Executes a query and writes its results to the data store.
	 *
	 * @param owner         The owner of the result
	 * @param hypothesisId  The id of the hypothesis to use as the basis for the query
	 * @param justification A justification for this query
	 * @param userid        The name of the user executing the query.
	 * @param visibilities    A list of string corresponding to allowed visibilities for the user.
	 * @return An AminoQueryResult containing the results of the query.
	 */
	Common.TQueryResult createResult(1: string owner, 2: string hypothesisId, 3: i32 maxResults, 4: string justification,
	 5: string userid, 6: set<string> visibilities);

	/**
	 * Deletes a QueryResult
	 *
	 * @param owner      The owner string of the QueryResult to delete
	 * @param id         The id of the QueryResult to delete
	 * @param visibilities A list of strings corresponding to allowed visibilities for the user.
	 */
	void deleteResult(1: string owner, 2: string id, 3: set<string> visibilities);

	/**
	 * Gets a count of results for a given range of feature values and a bucket
	 *
	 * @param featureId  The id of the feature
	 * @param bucketName The name of the bucket
	 * @param beginRange The start of the range (inclusive)
	 * @param endRange   The end of the range (inclusive)
	 * @param visibilities A list of strings corresponding to allowed visibilities for the user.
	 */
	i32 getCountForHypothesisFeature(1: string featureId, 2: string bucketName, 3: string beginRange, 4: string endRange,
	 5: set<string> visibilities);

    /**
     * Creates Hypotheses from the given bucket values.  Each of the hypotheses will be a Hypothesis that represents all
     * of the features that would match that bucket value for the given datasource and bucket.  The Hypotheses are only
     * created, they are not persisted to the database.  This call takes optional paramters to abort afer a specified
     * timeout value.
     *
     * @param datasourceid The datasource to check the bucket values against
     * @param bucketid The bucket to check the bucket values against
     * @param visibilities The Accumulo visibilities
     * @param userid the DN of the person making the request
     * @param justification The justification string for auditing
     * @param featureIds (optional) The featureIds that we are interested in. If provided, all others will be excluded from the results
     * @param timeout (optional) The amount of time to wait in seconds before timing out. If <= 0, then default will be used
     * @return A Collection of Hypothesis, one for each bucket value
     */
    list<Common.THypothesis> createNonPersistedHypothesisListForBucketValue(
            1: string datasourceid, 2: string bucketid, 3: list<string> bucketValues, 4: set<string> visibilities,
            5: string userid, 6: string justification, 7: list<string> featureIds, 8: i64 timeout);

    /**
     * Finds all existing, visible hypotheses that intersect with the given bucketValues.
     *
     * @param bvRequest All of the parameters
     * @return Hypotheses that intersect with the bucketvalues
     */
    list<Common.THypothesis> getHypothesesByBucketValues(1: Common.TByValuesRequest bvRequest);

    /**
     * Determines the uniqueness score of the given feature for a particular bucket
     * @param featureId The feature to look up
     * @param bucketName The bucket to look in
     * @param count The number of times that the feature exists for that bucket
     * @param visibilities The BigTable visibility strings
     * @return The uniqueness score
     * @throws BigTableException
     */
    double getUniqueness(1: string featureId, 2: string bucketName, 3: i32 count,
        4: set<string> visibilities);
}