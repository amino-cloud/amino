package com._42six.amino.query.services.accumulo;

import com._42six.amino.common.BucketMetadata;
import com._42six.amino.common.DatasourceMetadata;
import com._42six.amino.common.FeatureMetadata;
import com._42six.amino.common.entity.Hypothesis;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AccumuloMetadataServiceTest extends AccumuloTest {
	
	static AccumuloMetadataService metadataService;
	static AccumuloGroupService groupService;
	
	@BeforeClass
	public static void setupClass(){
		metadataService = new AccumuloMetadataService(persistenceService);
		groupService = new AccumuloGroupService(persistenceService);
	}
	
	@Ignore @Test
	public void testListDataSources() throws IOException {
		List<DatasourceMetadata> datasourceMetadatas = metadataService.listDataSources(visArray);
		assertTrue(datasourceMetadatas.size() > 0);
	}
	
	@Ignore @Test
	public void testListFeatures() throws IOException {
		List<DatasourceMetadata> datasourceMetadatas = metadataService.listDataSources(visArray);
		List<FeatureMetadata> features = metadataService.listFeatures(datasourceMetadatas.get(0).id, visArray);
		assertTrue(features.size() > 0);
	}
	
	@Ignore @Test
	public void testGetFeature() throws IOException {
		List<DatasourceMetadata> datasourceMetadatas = metadataService.listDataSources(visArray);
		List<FeatureMetadata> features = metadataService.listFeatures(datasourceMetadatas.get(0).id, visArray);
		FeatureMetadata f = metadataService.getFeature(features.get(0).id, visArray);
		assertNotNull(f);
	}
	
	@Ignore @Test
	public void testListBuckets() throws IOException {
		List<DatasourceMetadata> datasourceMetadatas = metadataService.listDataSources(visArray);
		List<BucketMetadata> buckets = metadataService.listBuckets(datasourceMetadatas.get(0).id, visArray);
		assertTrue(buckets.size() > 0);
	}
	
	@Ignore @Test
	public void testGetBucket() throws IOException {
		List<DatasourceMetadata> datasourceMetadatas = metadataService.listDataSources(visArray);
		List<BucketMetadata> buckets = metadataService.listBuckets(datasourceMetadatas.get(0).id, visArray);
		BucketMetadata b = metadataService.getBucket(buckets.get(0).id, visArray);
		assertNotNull(b);
	}
	
	@Test (expected=IllegalArgumentException.class) 
	public void listHypotheses_emptyOwner() throws IOException {
		List<Hypothesis> hypotheses = metadataService.listHypotheses("", visArray);
		assertTrue("size != 0", hypotheses.size() == 0);
	}
/*
	@Test
	public void listHypotheses(){
		List<Hypothesis> hypotheses = metadataService.listHypotheses(testOwnerPrefix+"1", visArray);
		assertEquals(2, hypotheses.size());
		 hypotheses = metadataService.listHypotheses(testOwnerPrefix+"2", visArray);
		 assertEquals(1, hypotheses.size());
	}

	@Ignore("MockBatchDeleter is not implemented!!") @Test
	public void createHypothesis() {
		Hypothesis h = createNominalHypothesis("Nom");
		h.owner = "Owner1";
		h = metadataService.createHypothesis(h, visArray);
		assertNotNull(h);

		metadataService.deleteHypothesis(h.owner, h.id, visArray);
	}
	
	@Ignore("MockBatchDeleter is not implemented!!") @Test (expected=EntityNotFoundException.class) 
	public void deleteHypothesis(){
		Hypothesis h =  createNominalHypothesis("Nom");
		h.owner = "Owner1";
		h = metadataService.createHypothesis(h, visArray);
		metadataService.deleteHypothesis(h.owner, h.id, visArray);
		metadataService.getHypothesis(h.owner, h.id, visArray);
	}
	
	@Ignore("MockBatchDeleter is not implemented!!") @Test
	public void updateHypothesis() {
		Hypothesis h =  createNominalHypothesis("Nom");
		h.owner = "Owner1";
	    h = metadataService.createHypothesis(h, visArray);
	    h.name = "Changed";
		h = metadataService.updateHypothesis(h, visArray);
		Hypothesis retrieved = metadataService.getHypothesis(h.owner, h.id, visArray);
		assertEquals(h.name, retrieved.name);
		metadataService.deleteHypothesis(retrieved.owner, retrieved.id, visArray);
	}
	*/
}
