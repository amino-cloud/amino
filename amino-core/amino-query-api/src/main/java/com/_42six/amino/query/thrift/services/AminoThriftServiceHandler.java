package com._42six.amino.query.thrift.services;


import com._42six.amino.common.BucketMetadata;
import com._42six.amino.common.DatasourceMetadata;
import com._42six.amino.common.FeatureMetadata;
import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.entity.QueryResult;
import com._42six.amino.common.thrift.*;
import com._42six.amino.common.translator.ThriftTranslator;
import com._42six.amino.query.exception.BigTableException;
import com._42six.amino.query.services.AminoGroupService;
import com._42six.amino.query.services.AminoMetadataService;
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

public class AminoThriftServiceHandler implements AminoThriftService.Iface{
    private AminoGroupService groupService;
    private AminoMetadataService metadataService;
    private AminoQueryService queryService;

    public AminoThriftServiceHandler(AminoGroupService groupService, AminoMetadataService metadataService, AminoQueryService queryService){
        this.groupService = groupService;
        this.metadataService = metadataService;
        this.queryService = queryService;
    }

    //
    // Query methods
    //

    /**
     * Lists existing AminoQueryResults for a given user.
     *
     * @param start     There are two parameters, both optional, used for paging. start defaults to 0 if not specified, and count defaults to 20 if not specified.
     * @param count     How many to return.
     * @param userid     The userid to search on.
     * @param visibilities A list of string corresponding to allowed visibilities for the user.
     * @return A list of AminoQueryResults.
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
     * @param maxResults    The maximum number of results to hit before giving up
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
     * @param bucketValues  The values to generate the Hypotheses from
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
                    bucketid, bucketValues, visibilities.toArray(new String[visibilities.size()]), userid, justification, featureIds,
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


    //
    // Metadata methods
    //

    private String[] toVisArray(Set<String> visibilities){
        return visibilities.toArray(new String[visibilities.size()]);
    }

    @Override
    public List<TDatasourceMetadata> listDataSources(Set<String> visibilities) throws TException {
        try {
            final List<DatasourceMetadata> results = metadataService.listDataSources(toVisArray(visibilities));
            final List<TDatasourceMetadata> retVal = new ArrayList<TDatasourceMetadata>(results.size());
            for(DatasourceMetadata d : results){
                retVal.add(ThriftTranslator.toTDatasourceMetadata(d));
            }
            return retVal;
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public List<TFeatureMetadata> listFeatures(String datasourceId, Set<String> visibilities) throws TException {
        try {
            final List<FeatureMetadata> results = metadataService.listFeatures(datasourceId, toVisArray(visibilities));
            final List<TFeatureMetadata> retVal = new ArrayList<TFeatureMetadata>(results.size());
            for(FeatureMetadata f : results){
                retVal.add(ThriftTranslator.toTFeatureMetadata(f));
            }
            return retVal;
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public List<TBucketMetadata> listBuckets(String datasourceId, Set<String> visibilities) throws TException {
        try {
            final List<BucketMetadata> results = metadataService.listBuckets(datasourceId, toVisArray(visibilities));
            final List<TBucketMetadata> retVal = new ArrayList<TBucketMetadata>(results.size());
            for(BucketMetadata b : results){
                retVal.add(ThriftTranslator.toTBucketMetadata(b));
            }
            return retVal;
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public TDatasourceMetadata getDataSource(String dataSourceId, Set<String> visibilities) throws TException {
        try {
            final DatasourceMetadata result = metadataService.getDataSource(dataSourceId, toVisArray(visibilities));
            return ThriftTranslator.toTDatasourceMetadata(result);
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public TFeatureMetadata getFeature(String id, Set<String> visibilities) throws TException {
        try {
            final FeatureMetadata result = metadataService.getFeature(id, toVisArray(visibilities));
            return ThriftTranslator.toTFeatureMetadata(result);
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public TBucketMetadata getBucket(String id, Set<String> visibilities) throws TException {
        try {
            final BucketMetadata result = metadataService.getBucket(id, toVisArray(visibilities));
            return ThriftTranslator.toTBucketMetadata(result);
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    /**
     * Fetches the Hypothesis
     *
     * @param userId       The ID of the user making the request
     * @param owner        The owner field of the hypthesis to fetch
     * @param hypothesisId The ID of the hypothesis to fetch
     * @param visibilities   The security visibilities for the database
     */
    @Override
    public THypothesis getHypothesis(String userId, String owner, String hypothesisId, Set<String> visibilities) throws TException {
        try {
            final Hypothesis result = metadataService.getHypothesis(userId, owner, hypothesisId, toVisArray(visibilities));
            return ThriftTranslator.toTHypothesis(result);
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public List<THypothesis> listHypotheses(String userId, Set<String> visibilities) throws TException {
        try {
            final List<Hypothesis> results = metadataService.listHypotheses(userId, toVisArray(visibilities));
            final List<THypothesis> retVal = new ArrayList<THypothesis>(results.size());
            for(Hypothesis h : results){
                retVal.add(ThriftTranslator.toTHypothesis(h));
            }
            return retVal;
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public THypothesis createHypothesis(THypothesis hypothesis, String userId, Set<String> visibilities) throws TException {
        try {
            final Hypothesis result = metadataService.createHypothesis(ThriftTranslator.fromTHypothesis(hypothesis),userId, toVisArray(visibilities));
            return ThriftTranslator.toTHypothesis(result);
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public THypothesis updateHypothesis(THypothesis hypothesis, String requester, Set<String> visibilities) throws TException {
        try {
            final Hypothesis result = metadataService.updateHypothesis(ThriftTranslator.fromTHypothesis(hypothesis), requester, toVisArray(visibilities));
            return ThriftTranslator.toTHypothesis(result);
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public void deleteHypothesis(String owner, String id, Set<String> visibilities) throws TException {
        metadataService.deleteHypothesis(owner, id, toVisArray(visibilities));
    }

    @Override
    public int getShardCount() throws TException {
        try {
            return metadataService.getShardCount();
        } catch (BigTableException e) {
            throw new TException(e);
        }
    }

    @Override
    public int getHashCount() throws TException {
        try {
            return metadataService.getHashCount();
        } catch (BigTableException e) {
            throw new TException(e);
        }
    }


    //
    // Group methods
    //

    @Override
    public boolean verifyGroupExists(String group, Set<String> visibilities) throws TException {
        try {
            return groupService.verifyGroupExists(group, visibilities);
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public boolean verifyUserExists(String user, Set<String> visibilities) throws TException {
        try {
            return groupService.verifyUserExists(user, visibilities);
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public void addToGroup(TAddUsersRequest request) throws TException {
        try {
            groupService.addToGroup(ThriftTranslator.fromTAddUsersRequest(request));
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public void createGroup(TCreateGroupRequest request) throws TException {
        try {
            groupService.createGroup(ThriftTranslator.fromTCreateGroupRequest(request));
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public Set<String> listGroups(String userId, Set<String> visibilities) throws TException {
        try {
            return groupService.listGroups(userId, visibilities);
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public Set<String> getGroupsForUser(String userId, Set<String> visibilities) throws TException {
        try {
            return groupService.getGroupsForUser(userId, visibilities);
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public void removeUserFromGroups(String requester, String userId, Set<String> groups, Set<String> visibilities) throws TException {
        try {
            groupService.removeUserFromGroups(requester, userId, groups, visibilities);
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public void removeUsersFromGroup(String requester, String group, Set<String> users, Set<String> visibilities) throws TException {
        try {
            groupService.removeUsersFromGroup(requester, group, users, visibilities);
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public List<THypothesis> getGroupHypothesesForUser(String userId, Set<String> visibilities, boolean userOwned) throws TException {
        try {
            final List<Hypothesis> hypotheses = groupService.getGroupHypothesesForUser(userId, visibilities, userOwned);
            final List<THypothesis> toReturn = new ArrayList<THypothesis>(hypotheses.size());
            for(Hypothesis h : hypotheses){
                toReturn.add(ThriftTranslator.toTHypothesis(h));
            }
            return toReturn;
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public TGroup getGroup(String requester, String group, Set<String> visibilities) throws TException {
        try {
            return ThriftTranslator.toTGroup(groupService.getGroup(requester, group, visibilities));
        } catch (IOException e) {
            throw new TException(e);
        }
    }
}
