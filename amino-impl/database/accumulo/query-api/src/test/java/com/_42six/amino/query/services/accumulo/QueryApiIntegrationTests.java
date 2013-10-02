package com._42six.amino.query.services.accumulo;

import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.entity.HypothesisFeature;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.FileInputStream;
import java.util.Properties;

public abstract class QueryApiIntegrationTests {
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        final Properties props = new Properties();
        props.load(new FileInputStream("target/classes/bigtable.properties"));

        final String instance = props.getProperty("bigtable.instance");
        final String zookeepers = props.getProperty("bigtable.zookeepers");
        final String user = props.getProperty("bigtable.user");
        final String password = props.getProperty("bigtable.password");

        try {
            persistenceService = new AccumuloPersistenceService(instance, zookeepers, user, password);
            metadataService = new AccumuloMetadataService(persistenceService);
            queryService = new AccumuloQueryService(persistenceService, metadataService);
            visibility = persistenceService.getLoggedInUserAuthorizations().toString().split(",");
        } catch (Exception e) {
            System.out.println("Caught Exception " + e.getMessage());
            connectedToAccumulo = false;
        }
    }

    protected Hypothesis configureHypothesis() {
        Hypothesis h = new Hypothesis();
        h.bucketid = "1025254987";
        h.datasourceid = "Number Domain";
        h.name = "Even";

        return h;
    }

    protected void addNominalFeatureToHypothesis(Hypothesis hypothesis, String featureId, String featureValue) {
        HypothesisFeature f = new HypothesisFeature();
        f.featureMetadataId = featureId;
        f.type = "NOMINAL";
        f.value = featureValue;
        hypothesis.hypothesisFeatures.add(f);
    }

    protected void addRatioFeatureToHypothesis(Hypothesis hypothesis, String featureId, double featureMin, double featureMax) {
        HypothesisFeature f = new HypothesisFeature();
        f.featureMetadataId = featureId;
        f.type = "RATIO";
        f.min = featureMin;
        f.max = featureMax;
        hypothesis.hypothesisFeatures.add(f);
    }

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(connectedToAccumulo);
    }

    public static AccumuloPersistenceService getPersistenceService() {
        return persistenceService;
    }

    public static void setPersistenceService(AccumuloPersistenceService persistenceService) {
        QueryApiIntegrationTests.persistenceService = persistenceService;
    }

    public static AccumuloMetadataService getMetadataService() {
        return metadataService;
    }

    public static void setMetadataService(AccumuloMetadataService metadataService) {
        QueryApiIntegrationTests.metadataService = metadataService;
    }

    public static AccumuloQueryService getQueryService() {
        return queryService;
    }

    public static void setQueryService(AccumuloQueryService queryService) {
        QueryApiIntegrationTests.queryService = queryService;
    }

    public static String[] getVisibility() {
        return visibility;
    }

    public static void setVisibility(String[] visibility) {
        QueryApiIntegrationTests.visibility = visibility;
    }

    private static AccumuloPersistenceService persistenceService;
    private static AccumuloMetadataService metadataService;
    private static AccumuloQueryService queryService;
    private static String[] visibility;
    private static boolean connectedToAccumulo = true;
}
