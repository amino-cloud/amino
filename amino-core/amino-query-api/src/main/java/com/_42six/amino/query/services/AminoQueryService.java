package com._42six.amino.query.services;

import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.entity.QueryResult;
import com._42six.amino.common.query.requests.bta.BtaByValuesRequest;
import com._42six.amino.query.exception.BigTableException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Defines the contract for executing queries against the Amino data store.
 *
 * @author Amino Team
 */
public interface AminoQueryService {
    /**
     * Adds the suffix to all of the tables
     * @param suffix The suffix to append to the tables
     */
    public void addTableSuffix(String suffix);

	/**
	 * Lists existing AminoQueryResults for a given user.
	 *
	 * @param start     There are two parameters, both optional, used for paging. start defaults to 0 if not specified, and count defaults to 20 if not specified.
     * @param count     How many to return.
	 * @param userid     The userid to search on.
	 * @param visibility A list of string corresponding to allowed visibilities for the user.
	 * @return A list of AminoQueryResults.
	 */
	public List<QueryResult> listResults(Long start, Long count, String userid, String[] visibility) throws IOException;

	/**
	 * Get an already existing result from the data store.
	 *
	 * @param requester  The ID String of the person making the request
	 * @param owner      The owner string of the QueryResult
	 * @param id         The id of the result.
	 * @param visibility A list of string corresponding to allowed visibilities for the user.
	 * @return The filled out AminoQueryResults.
	 */
	public QueryResult getResult(String requester, String owner, String id, String[] visibility) throws Exception;

	/**
	 * Executes a query and writes its results to the data store.
	 *
	 * @param owner         The owner of the result
	 * @param hypothesisId  The id of the hypothesis to use as the basis for the query
     * @param maxResults    The maximum number of results to hit before giving up
	 * @param justification A justification for this query
	 * @param userid        The name of the user executing the query.
	 * @param visibility    A list of string corresponding to allowed visibilities for the user.
	 * @return An AminoQueryResult containing the results of the query.
	 */
	public QueryResult createResult(String owner, String hypothesisId, Integer maxResults, String justification, String userid, String[] visibility) throws InterruptedException, ExecutionException, TimeoutException;

	/**
	 * Deletes a QueryResult
	 *
	 * @param owner      The owner string of the QueryResult to delete
	 * @param id         The id of the QueryResult to delete
	 * @param visibility A list of string corresponding to allowed visibilities for the user.
	 */
	public void deleteResult(String owner, String id, String[] visibility) throws Exception;

	/**
	 * Gets a count of results for a given range of feature values and a bucket
	 *
	 * @param featureId  The id of the feature
	 * @param bucketName The name of the bucket
	 * @param beginRange The start of the range (inclusive)
	 * @param endRange   The end of the range (inclusive)
	 */
	public Integer getCountForHypothesisFeature(String featureId, String bucketName, String beginRange, String endRange, String[] visibility) throws Exception;

    /**
     * Creates Hypotheses from the given bucket values.  Each of the hypotheses will be a Hypothesis that represents all
     * of the features that would match that bucket value for the given datasource and bucket.  The Hypotheses are only
     * created, they are not persisted to the database.  This call takes optional paramters to abort afer a specified
     * timeout value.
     *
     * @param datasourceid The datasource to check the bucket values against
     * @param bucketid The bucket to check the bucket values against
     * @param bucketValues The values to generate the Hypotheses from
     * @param visibility The Accumulo visibilities
     * @param userid the DN of the person making the request
     * @param justification The justification string for auditing
     * @param featureIds (optional) The featureIds that we are interested in. If provided, all others will be excluded from the results
     * @param timeout (optional) The amount of time to wait before timing out. If <= 0, then default will be used
     * @param units (optional) The TimeUnit of the timeout. Defaults to seconds
     * @return A Collection of Hypothesis, one for each bucket value
     */
    public Collection<Hypothesis> createNonPersistedHypothesisListForBucketValue(
            String datasourceid, String bucketid, Collection<String> bucketValues, String[] visibility, String userid,
            String justification, List<String> featureIds, long timeout, TimeUnit units) throws InterruptedException, ExecutionException, TimeoutException;

    /**
     * Finds all existing, visible hypotheses that intersect with the given bucketValues.
     *
     * @param bvRequest All of the parameters
     * @return Hypotheses that intersect with the bucketvalues
     */
    public List<Hypothesis> getHypothesesByBucketValues(BtaByValuesRequest bvRequest) throws InterruptedException, ExecutionException, TimeoutException;

    /**
     * Determines the uniqueness score of the given feature for a particular bucket
     * @param featureId The feature to look up
     * @param bucketName The bucket to look in
     * @param count The number of times that the feature exists for that bucket
     * @param visibility The Accumulo visibility strings
     * @return The uniqueness score
     * @throws BigTableException
     */
    public double getUniqueness(String featureId, String bucketName, Integer count, String[] visibility) throws BigTableException;


}
