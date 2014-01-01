package com._42six.amino.query.thrift.services;

import com._42six.amino.common.BucketMetadata;
import com._42six.amino.common.DatasourceMetadata;
import com._42six.amino.common.FeatureMetadata;
import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.thrift.TBucketMetadata;
import com._42six.amino.common.thrift.TDatasourceMetadata;
import com._42six.amino.common.thrift.TFeatureMetadata;
import com._42six.amino.common.thrift.THypothesis;
import com._42six.amino.common.translator.ThriftTranslator;
import com._42six.amino.query.exception.BigTableException;
import com._42six.amino.query.services.AminoMetadataService;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ThriftMetadataServiceHandler implements ThriftMetadataService.Iface {

    private AminoMetadataService metadataService;

    public ThriftMetadataServiceHandler(AminoMetadataService metadataService){
        this.metadataService = metadataService;
    }

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
}
