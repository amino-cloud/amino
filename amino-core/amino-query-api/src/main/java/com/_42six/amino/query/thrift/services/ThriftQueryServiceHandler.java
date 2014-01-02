package com._42six.amino.query.thrift.services;

import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.entity.QueryResult;
import com._42six.amino.common.thrift.TByValuesRequest;
import com._42six.amino.common.thrift.THypothesis;
import com._42six.amino.common.thrift.TQueryResult;
import com._42six.amino.common.translator.ThriftTranslator;
import com._42six.amino.query.exception.BigTableException;
import com._42six.amino.query.services.AminoQueryService;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ThriftQueryServiceHandler implements ThriftQueryService.Iface {

    private AminoQueryService queryService;

    public ThriftQueryServiceHandler(AminoQueryService queryService){
        this.queryService = queryService;
    }

    /**
     * Lists existing AminoQueryResults for a given user.
     *
     * @param start     There are two parameters, both optional, used for paging. start defaults to 0 if not specified, and count defaults to 20 if not specified.
     * @param count     How many to return.
     * @param userid     The userid to search on.
     * @param visibilities A list of string corresponding to allowed visibilities for the user.
     * @return A list of AminoQueryResults.
     *
     * @param start
     * @param count
     * @param userid
     * @param visibilities
     */
    @Override
    public List<TQueryResult> listResults(long start, long count, String userid, Set<String> visibilities) throws TException {
        try {
            final List<QueryResult> results = queryService.listResults(start, count, userid, visibilities.toArray(new String[visibilities.size()]));
            final List<TQueryResult> retVal = new ArrayList<TQueryResult>(results.size());
            for(QueryResult result : results){
                retVal.add(ThriftTranslator.toTQueryResult(result));
            }
            return retVal;
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    /**
     * Get an already existing result from the data store.
     *
     * @param requester    The ID String of the person making the request
     * @param owner        The owner string of the QueryResult
     * @param id           The id of the result.
     * @param visibilities A list of string corresponding to allowed visibilities for the user.
     * @return The filled out AminoQueryResults.
     */
    @Override
    public TQueryResult getResult(String requester, String owner, String id, Set<String> visibilities) throws TException {
        try {
            final QueryResult result = queryService.getResult(requester, owner, id, visibilities.toArray(new String[visibilities.size()]));
            return ThriftTranslator.toTQueryResult(result);
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    /**
     * Executes a query and writes its results to the data store.
     *
     * @param owner         The owner of the result
     * @param hypothesisId  The id of the hypothesis to use as the basis for the query
     * @param maxResults
     * @param justification A justification for this query
     * @param userid        The name of the user executing the query.
     * @param visibilities  A list of string corresponding to allowed visibilities for the user.
     * @return An AminoQueryResult containing the results of the query.
     */
    @Override
    public TQueryResult createResult(String owner, String hypothesisId, int maxResults, String justification, String userid, Set<String> visibilities) throws TException {
        try {
            final QueryResult result = queryService.createResult(owner, hypothesisId, maxResults, justification, userid, visibilities.toArray(new String[visibilities.size()]));
            return ThriftTranslator.toTQueryResult(result);
        } catch (InterruptedException e) {
            throw new TException(e);
        } catch (ExecutionException e) {
            throw new TException(e);
        } catch (TimeoutException e) {
            throw new TException(e);
        }
    }

    /**
     * Deletes a QueryResult
     *
     * @param owner        The owner string of the QueryResult to delete
     * @param id           The id of the QueryResult to delete
     * @param visibilities A list of strings corresponding to allowed visibilities for the user.
     */
    @Override
    public void deleteResult(String owner, String id, Set<String> visibilities) throws TException {
        try {
            queryService.deleteResult(owner, id, visibilities.toArray(new String[visibilities.size()]));
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    /**
     * Gets a count of results for a given range of feature values and a bucket
     *
     * @param featureId    The id of the feature
     * @param bucketName   The name of the bucket
     * @param beginRange   The start of the range (inclusive)
     * @param endRange     The end of the range (inclusive)
     * @param visibilities A list of strings corresponding to allowed visibilities for the user.
     */
    @Override
    public int getCountForHypothesisFeature(String featureId, String bucketName, String beginRange, String endRange, Set<String> visibilities) throws TException {
        try {
            return queryService.getCountForHypothesisFeature(featureId, bucketName, beginRange, endRange, visibilities.toArray(new String[visibilities.size()]));
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    /**
     * Creates Hypotheses from the given bucket values.  Each of the hypotheses will be a Hypothesis that represents all
     * of the features that would match that bucket value for the given datasource and bucket.  The Hypotheses are only
     * created, they are not persisted to the database.  This call takes optional parameters to abort after a specified
     * timeout value.
     *
     * @param datasourceid  The datasource to check the bucket values against
     * @param bucketid      The bucket to check the bucket values against
     * @param bucketValues
     * @param visibilities  The Accumulo visibilities
     * @param userid        the DN of the person making the request
     * @param justification The justification string for auditing
     * @param featureIds    (optional) The featureIds that we are interested in. If provided, all others will be excluded from the results
     * @param timeout       (optional) The amount of time to wait in seconds before timing out. If <= 0, then default will be used
     * @return A Collection of Hypothesis, one for each bucket value
     */
    @Override
    public List<THypothesis> createNonPersistedHypothesisListForBucketValue(String datasourceid, String bucketid, List<String> bucketValues, Set<String> visibilities, String userid, String justification, List<String> featureIds, long timeout) throws TException {
        try {
            final Collection<Hypothesis> results = queryService.createNonPersistedHypothesisListForBucketValue(datasourceid,
                    bucketid, bucketValues, visibilities.toArray(new String[0]), userid, justification, featureIds,
                    timeout, TimeUnit.SECONDS);
            final List<THypothesis> retVal = new ArrayList<THypothesis>(results.size());
            for(Hypothesis hypothesis : results) {
                retVal.add(ThriftTranslator.toTHypothesis(hypothesis));
            }
            return retVal;
        } catch (InterruptedException e) {
            throw new TException(e);
        } catch (ExecutionException e) {
            throw new TException(e);
        } catch (TimeoutException e) {
            throw new TException(e);
        }
    }

    /**
     * Finds all existing, visible hypotheses that intersect with the given bucketValues.
     *
     * @param bvRequest All of the parameters
     * @return Hypotheses that intersect with the bucketvalues
     */
    @Override
    public List<THypothesis> getHypothesesByBucketValues(TByValuesRequest bvRequest) throws TException {
        final List<Hypothesis> results;
        try {
            results = queryService.getHypothesesByBucketValues(ThriftTranslator.fromTByValuesRequest(bvRequest));
        } catch (InterruptedException e) {
            throw new TException(e);
        } catch (ExecutionException e) {
            throw new TException(e);
        } catch (TimeoutException e) {
            throw new TException(e);
        }

        final List<THypothesis> retVal = new ArrayList<THypothesis>(results.size());
        for(Hypothesis hypothesis : results){
            retVal.add(ThriftTranslator.toTHypothesis(hypothesis));
        }
        return retVal;
    }

    /**
     * Determines the uniqueness score of the given feature for a particular bucket
     *
     * @param featureId    The feature to look up
     * @param bucketName   The bucket to look in
     * @param count        The number of times that the feature exists for that bucket
     * @param visibilities The BigTable visibility strings
     * @return The uniqueness score
     */
    @Override
    public double getUniqueness(String featureId, String bucketName, int count, Set<String> visibilities) throws TException {
        try {
            return queryService.getUniqueness(featureId, bucketName, count, visibilities.toArray(new String[visibilities.size()]));
        } catch (BigTableException e) {
            throw new TException(e);
        }
    }
}
