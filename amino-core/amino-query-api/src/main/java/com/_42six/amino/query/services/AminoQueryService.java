package com._42six.amino.query.services;

import com._42six.amino.common.entity.QueryResult;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Defines the contract for executing queries against the Amino data store.
 *
 * @author Amino Team
 */
public interface AminoQueryService {
	/**
	 * Lists exiting AminoQueryResults for a given user.
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
}
