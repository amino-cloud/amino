package com._42six.amino.query.services.accumulo;

public class AccumuloQueryServiceIT extends QueryApiIntegrationTests {
//    @Test
//    @Ignore
//    public void createResult() {
//        Hypothesis hypothesis = configureHypothesis();
//        addNominalFeatureToHypothesis(hypothesis, "1186613585", "even");
//
//        Hypothesis h = QueryApiIntegrationTests.getMetadataService().createHypothesis(hypothesis, QueryApiIntegrationTests.getVisibility());
//        QueryResult result = DefaultGroovyMethods.invokeMethod(QueryApiIntegrationTests.getQueryService(), "createResult", new Object[]{h.id, 3000, "SomeUser1", QueryApiIntegrationTests.getVisibility()});
//        Assert.assertNotNull(result);
//        Assert.assertTrue((boolean) result.result_count > 0);
//        Assert.assertEquals(500, result.result_count);
//    }
//
//    @Test
//    @Ignore
//    public void createResultWithMultipleFeatures() {
//        Hypothesis hypothesis = configureHypothesis();
//        addNominalFeatureToHypothesis(hypothesis, "1186613585", "even");
//        addRatioFeatureToHypothesis(hypothesis, "1668757391", 8, 9);
//        addRatioFeatureToHypothesis(hypothesis, "628381357", 940, 950);
//
//        Hypothesis h = QueryApiIntegrationTests.getMetadataService().createHypothesis(hypothesis, QueryApiIntegrationTests.getVisibility());
//        QueryResult result = DefaultGroovyMethods.invokeMethod(QueryApiIntegrationTests.getQueryService(), "createResult", new Object[]{h.id, 3000, "SomeUser1", QueryApiIntegrationTests.getVisibility()});
//
//        Assert.assertNotNull(result);
//        Assert.assertTrue((boolean) result.result_count > 0);
//        Assert.assertEquals(6, result.result_count);
//    }
//
//    @Test
//    @Ignore
//    public void getCountForHypothesisFeature() {
//        Integer result = QueryApiIntegrationTests.getQueryService().getCountForHypothesisFeature("1186613585", "number", "odd", "odd", QueryApiIntegrationTests.getVisibility());
//        Assert.assertNotNull(result);
//        Assert.assertEquals(500, result);
//    }
//
//    @Test
//    @Ignore
//    public void getFirstAndLastBitmaskScanInformationForQuery() {
//        Hypothesis hypothesis = configureHypothesis();
//        addNominalFeatureToHypothesis(hypothesis, "1186613585", "even");
//        addRatioFeatureToHypothesis(hypothesis, "1668757391", 8, 9);
//        addRatioFeatureToHypothesis(hypothesis, "628381357", 940, 950);
//
//        Hypothesis h = QueryApiIntegrationTests.getMetadataService().createHypothesis(hypothesis, QueryApiIntegrationTests.getVisibility());
//        Object info = QueryApiIntegrationTests.getQueryService().getBitmaskScanInformationForQuery(h.hypothesisFeatures, "number", new Authorizations(QueryApiIntegrationTests.getVisibility()));
//
//        Assert.assertEquals("940", info.first);
//        Assert.assertEquals("950", info.last);
//    }
//
//    @Test
//    public void createNonPersistedHypothesisForBucketValue() {
//        Hypothesis h = DefaultGroovyMethods.invokeMethod(QueryApiIntegrationTests.getQueryService(), "createNonPersistedHypothesisForBucketValue", new Object[]{"NFL_PBP", "1110613710", "J.Flacco", QueryApiIntegrationTests.getVisibility()});
//        Assert.assertTrue((boolean) h.hypothesisFeatures.size() > 0);
//    }
//
//    @Test
//    public void testSomething() {
//        Hypothesis h = QueryApiIntegrationTests.getMetadataService().createHypothesis(configureHypothesis(), QueryApiIntegrationTests.getVisibility());
//        QueryResult qr = DefaultGroovyMethods.invokeMethod(QueryApiIntegrationTests.getQueryService(), "deprecatedCreateResult", new Object[]{h.id, "SomeUser1", QueryApiIntegrationTests.getVisibility()});
//        for (QueryEntry q : qr.result_set) {
//            System.out.println(q.bucketName);
//        }
//
//    }
//
//    @Test(expected = getProperty("EntityNotFoundException").getClass())
//    public void testQueryResultDeletion() {
//        Hypothesis h = QueryApiIntegrationTests.getMetadataService().createHypothesis(configureHypothesis(), QueryApiIntegrationTests.getVisibility());
//        Assert.assertNotNull(h.id);
//        QueryResult qr = DefaultGroovyMethods.invokeMethod(QueryApiIntegrationTests.getQueryService(), "createResult", new Object[]{h.id, "SomeUser1", QueryApiIntegrationTests.getVisibility()});
//        QueryResult qrToTest = QueryApiIntegrationTests.getQueryService().getResult(qr.id, QueryApiIntegrationTests.getVisibility());
//        Assert.assertNotNull(qrToTest.id);
//        QueryApiIntegrationTests.getQueryService().deleteResult(qrToTest.id, (String) QueryApiIntegrationTests.getVisibility(), new String[0]);
//        qrToTest = QueryApiIntegrationTests.getQueryService().getResult(qr.id, QueryApiIntegrationTests.getVisibility());
//    }
//
//    @Test
//    public void listResults() {
//        Hypothesis h = QueryApiIntegrationTests.getMetadataService().createHypothesis(configureHypothesis(), QueryApiIntegrationTests.getVisibility());
//        DefaultGroovyMethods.invokeMethod(QueryApiIntegrationTests.getQueryService(), "createResult", new Object[]{h.id, "SomeUser1", QueryApiIntegrationTests.getVisibility()});
//        List<QueryResult> results = QueryApiIntegrationTests.getQueryService().listResults(new HashMap<String, Object>(), "SomeUser1", QueryApiIntegrationTests.getVisibility());
//        Assert.assertTrue((boolean) results.size() > 0);
//    }
//
//    @Test
//    public void getResult() {
//        Hypothesis h = QueryApiIntegrationTests.getMetadataService().createHypothesis(configureHypothesis(), QueryApiIntegrationTests.getVisibility());
//        QueryResult result = DefaultGroovyMethods.invokeMethod(QueryApiIntegrationTests.getQueryService(), "createResult", new Object[]{h.id, "SomeUser1", QueryApiIntegrationTests.getVisibility()});
//        QueryResult fetched = QueryApiIntegrationTests.getQueryService().getResult(result.id, QueryApiIntegrationTests.getVisibility());
//        Assert.assertNotNull(fetched);
//        Assert.assertEquals(result.result_count, fetched.result_count);
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void testResultWithNoFeatures() {
//        DefaultGroovyMethods.invokeMethod(QueryApiIntegrationTests.getQueryService(), "testResult", new Object[]{"1182638844", new ArrayList<HypothesisFeature>(), QueryApiIntegrationTests.getVisibility()});
//    }
//
//    @Test
//    public void testResultMultiple() {
//        List<HypothesisFeature> features = new ArrayList<HypothesisFeature>();
//        List<HypothesisFeature> features2 = new ArrayList<HypothesisFeature>();
//        List<HypothesisFeature> featuresTogether = new ArrayList<HypothesisFeature>();
//
//        HypothesisFeature f = new HypothesisFeature();
//        f.value = "Unnecessary Roughness";
//        f.type = "NOMINAL";
//        f.featureMetadataId = "308075115";//NFL_PBP feature:308075115 [] {"name":"This penalty was called when in the game","description":"This penalty was called when in the game.","namespace":"Public","type":"NOMINAL"}
//
//        features.add(f);
//        featuresTogether.add(f);
//
//        f = new HypothesisFeature();
//        f.value = "Offensive Holding";
//        f.type = "NOMINAL";
//        f.featureMetadataId = "308075115";//NFL_PBP feature:308075115 [] {"name":"This penalty was called when in the game","description":"This penalty was called when in the game.","namespace":"Public","type":"NOMINAL"}
//
//        features2.add(f);
//        featuresTogether.add(f);
//
//
//        QueryResult result = DefaultGroovyMethods.invokeMethod(QueryApiIntegrationTests.getQueryService(), "deprecatedTestResult", new Object[]{"1182638844", features, QueryApiIntegrationTests.getVisibility()});//NFL_PBP bucket:1182638844 []    {"name":"INT NAM","displayName":"Interceptor"}
//        QueryResult result2 = DefaultGroovyMethods.invokeMethod(QueryApiIntegrationTests.getQueryService(), "deprecatedTestResult", new Object[]{"1182638844", features2, QueryApiIntegrationTests.getVisibility()});//NFL_PBP bucket:1182638844 []    {"name":"INT NAM","displayName":"Interceptor"}
//        QueryResult resultTogether = DefaultGroovyMethods.invokeMethod(QueryApiIntegrationTests.getQueryService(), "deprecatedTestResult", new Object[]{"1182638844", featuresTogether, QueryApiIntegrationTests.getVisibility()});//NFL_PBP bucket:1182638844 []    {"name":"INT NAM","displayName":"Interceptor"}
//
//
//        System.out.println(result.result_count);
//        System.out.println(result2.result_count);
//        System.out.println(resultTogether.result_count);
//
//        Assert.assertTrue(result.result_count > 0);
//    }
//
//    @Test
//    public void testResultNominal() {
//        //queryService.testResult(String bucketid, String featuresInJsonFormat, String[] visibility);
//
//        List<HypothesisFeature> features = new ArrayList<HypothesisFeature>();
//
//        HypothesisFeature f = new HypothesisFeature();
//        f.value = "Unnecessary Roughness";
//        f.type = "NOMINAL";
//        f.featureMetadataId = "308075115";//NFL_PBP feature:308075115 [] {"name":"This penalty was called when in the game","description":"This penalty was called when in the game.","namespace":"Public","type":"NOMINAL"}
//
//        features.add(f);
//
//        QueryResult result = DefaultGroovyMethods.invokeMethod(QueryApiIntegrationTests.getQueryService(), "testResult", new Object[]{"1182638844", features, QueryApiIntegrationTests.getVisibility()});//NFL_PBP bucket:1182638844 []    {"name":"INT NAM","displayName":"Interceptor"}
//
//        System.out.println(result.result_count);
//
//        Assert.assertTrue((boolean) result.result_count > 0);
//    }
//
//    @Test
//    public void testResultRatio() {
//
//        List<HypothesisFeature> features = new ArrayList<HypothesisFeature>();
//
//        HypothesisFeature f;
//
//        //		f = new HypothesisFeature();
////		f.min = 0;
////		f.max = 0;
////		f.type = "RATIO";
////		f.featureMetadataId = "1621677302"; //NFL_PBP feature:1621677302 []    {"name":"Total TDs","description":"Total TDs when on the field","namespace":"Scoring Features","type":"RATIO"}
////
////		features.add(f);
//
////		f = new HypothesisFeature();
////		f.min = 1.0d;
////		f.max = 100000.0d;
////		f.type = "RATIO";
////		f.featureMetadataId = "1621677302"; //NFL_PBP feature:1621677302 []    {"name":"Total TDs","description":"Total TDs when on the field","namespace":"Scoring Features","type":"RATIO"}
////		features.add(f);
//
////		f = new HypothesisFeature();
////		f.min = 10d;
////		f.max = 10000d;
////		f.type = "RATIO";
////		f.featureMetadataId = "47244801"; //NFL_PBP feature:47244801 []    {"name":"Average Rushing Yards","description":"Average Rushing Yards","namespace":"Rushing Features","type":"RATIO"}
////		features.add(f);
//
//        f = new HypothesisFeature();
//        f.min = 111.01;
//        f.max = 111.01;
//        f.type = "RATIO";
//        f.featureMetadataId = "1070065069";//NFL_PBP feature:1070065069 []    {"name":"TD per pass ratio","description":"TDs per pass play when on the field.","namespace":"Scoring Features","type":"RATIO","min":0.0,"max":1.0}
//        features.add(f);
//
//        QueryResult result = DefaultGroovyMethods.invokeMethod(QueryApiIntegrationTests.getQueryService(), "testResult", new Object[]{"1110613710", features, QueryApiIntegrationTests.getVisibility()});//NFL_PBP bucket:1110613710 []    {"name":"PSR NAM","displayName":"Passer"}
//        //QueryResult result = queryService.testResult("1294867941", features, visibility); //NFL_PBP bucket:1294867941 []    {"name":"BC NAM","displayName":"Ball Carrier"}
//
//        System.out.println(result.result_count);
//
//        Assert.assertTrue((boolean) result.result_count > 0);
//    }

}
