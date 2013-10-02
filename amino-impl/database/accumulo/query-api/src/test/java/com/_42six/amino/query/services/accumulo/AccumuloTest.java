package com._42six.amino.query.services.accumulo;

import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.entity.HypothesisFeature;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public abstract class AccumuloTest {
	static Instance instance;
	static Connector conn;
	static Authorizations auths;
	final static String user = "user";
	final static String password = "password";
	static AccumuloPersistenceService persistenceService;
	public static final String visibilityString = "U";
	public static final String[] visArray = { visibilityString };
	
	
	private static final ColumnVisibility colVis =new ColumnVisibility(visibilityString);
	
	// The tables
	public final static String hypothesisTable = "amino_hypothesis"; 
	public final static String hypothesisByFieldTable = "amino_hypothesis_byField";
	public final static String metadataTable = "amino_metadata";
	public final static String bitLookupTable = "amino_bitmap_bitLookup";
	public final static String byBucketTable = "amino_bitmap_byBucket";
	public final static String resultsTable = "amino_query_result";
	public final static String featureLookupTable = "amino_feature_lookup";
	public final static String groupHypothesisLookupTable = "amino_group_hypothesis_lookup";
	public final static String groupMembershipTable =  "amino_group_membership";
	public static final String testTable = "test_table";
	
	public static final String testDatasourceId = "Test Datasource ID";
	public static final String testBucketId = "Test Bucket ID";
	public static final String testOwnerPrefix = "TestOwner";
	
	// HypothesisFeature type IDs
	public static final String testRatioId = "Test Ratio ID";
	public static final String testNominalId = "Test Nominal ID"; 
	public static final String testRestrictionId = "Test Restriction ID";
	
    // BatchWriter configuration - Stolen from BTPersistence class
	public final static Long BATCHWRITER_MAXMEMORY = 1000000L;
	public final static Long BATCHWRITER_MAXLATENCY = 1000L;
	public final static int BATCHWRITER_MAXWRITETHREADS = 10;

	//BatchScanner configuration - Stolen from BTPersistence class
    public final static Integer BATCHSCANNER_NUMQUERYTHREADS = 15;
	
	/***********************
	 * Setup Methods
	 **********************/
	
	@BeforeClass
	public static void testSetup() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, IOException {
		final Authorizations authorizations = new Authorizations(visibilityString);

		persistenceService = new AccumuloPersistenceService("instanceName", "BLERG", user, password, true);
		conn = persistenceService.getConnector();
		conn.securityOperations().createUser(user, password.getBytes(), authorizations);
		conn.securityOperations().changeUserAuthorizations(user, authorizations);
		auths = conn.securityOperations().getUserAuthorizations(user);
		assertEquals(auths, authorizations);

		// Create tables and Insert dummy data
		deleteAndCreateTable(testTable);
		resetHypothesisTable();
		resetGroupMembershipTable();
		resetMetadataTable();
	}
	
	@AfterClass
	public static void testCleanup() {
		instance = null;
		conn = null;
	}
	
    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(conn != null);
    }
	
	/***********************
	 * Helper Methods
	 **********************/
		
    private Hypothesis createHypothesis() {
        Hypothesis h = new Hypothesis();
        h.bucketid = testBucketId;
        h.datasourceid = testDatasourceId;
        h.name = "Test Hypothesis";
        h.justification = "Test Justification";
        h.btVisibility = visibilityString;
        h.visibility = visibilityString;
        h.canEdit = new ArrayList<String>();
        h.canView = new ArrayList<String>();
        h.hypothesisFeatures = new HashSet<HypothesisFeature>();

        return h;
    }

    protected Hypothesis createNominalHypothesis(String featureValue){
    	final Hypothesis h = createHypothesis();
    	return addNominalFeatureToHypothesis(h, featureValue);
    }
    
    protected Hypothesis createRatioHypothesis(double min, double max){
    	final Hypothesis h = createHypothesis();
    	return addRatioFeatureToHypothesis(h, min, max);
    }
    
    protected Hypothesis createRestrictionHypothesis(String restrictions){
    	final Hypothesis h = createHypothesis();
    	return addRestrictionFeatureToHypothesis(h, restrictions);
    }
    
    protected Hypothesis addRestrictionFeatureToHypothesis(Hypothesis hypothesis, String restrictions){
        final HypothesisFeature f = new HypothesisFeature();
        f.featureMetadataId = testRestrictionId;
        f.type = "RESTRICTION";
        f.value = restrictions;
        hypothesis.hypothesisFeatures.add(f);
        return hypothesis;
    }
    
    protected Hypothesis addNominalFeatureToHypothesis(Hypothesis hypothesis, String featureValue){
        final HypothesisFeature f = new HypothesisFeature();
        f.featureMetadataId = testNominalId;
        f.type = "NOMINAL";
        f.value = featureValue;
        hypothesis.hypothesisFeatures.add(f);
        return hypothesis;
    }

    protected Hypothesis addRatioFeatureToHypothesis(Hypothesis hypothesis, double featureMin, double featureMax) {
        final HypothesisFeature f = new HypothesisFeature();
        f.featureMetadataId = testRatioId;
        f.type = "RATIO";
        f.min = featureMin;
        f.max = featureMax;
        hypothesis.hypothesisFeatures.add(f);
        return hypothesis;
    }
	
    /**
     * Removes the table and then re-creates it with no values
     * @param tableName The name of the table
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
	protected static void deleteAndCreateTable(String tableName) throws AccumuloException, AccumuloSecurityException{
		try {
			conn.tableOperations().delete(tableName);
		}  catch (TableNotFoundException e) {
			// Don't care if the table wasn't found
		}
		
		try {
			conn.tableOperations().create(tableName);
		} catch (TableExistsException e) {
			// This will never happen because we delete the table first
		}		
	}
	
	protected  static BatchWriter createBatchWriter(String tableName) throws TableNotFoundException{
		return conn.createBatchWriter(tableName, BATCHWRITER_MAXMEMORY, BATCHWRITER_MAXLATENCY, BATCHWRITER_MAXWRITETHREADS);		
	}

	/************************
	* TABLE BUILDING
	************************/
//	conn.tableOperations().create(hypothesisByFieldTable);
//	conn.tableOperations().create(metadataTable);
//	conn.tableOperations().create(bitLookupTable);
//	conn.tableOperations().create( byBucketTable);
//	conn.tableOperations().create(resultsTable);
//	conn.tableOperations().create( featureLookupTable);
//	conn.tableOperations().create(groupHypothesisLookupTable);
	
	/**
	 * Erases and re-creates the amino_group_membership table
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	public static void resetGroupMembershipTable() throws AccumuloException, AccumuloSecurityException, TableNotFoundException{
		deleteAndCreateTable(groupMembershipTable);
		
		// Insert data
		final BatchWriter writer = createBatchWriter(groupMembershipTable);
		Mutation m = new Mutation("Owner1");
		m.put("group1", "",colVis, "");
		m.put("group2", "",colVis, "");
		writer.addMutation(m);
		
		m = new Mutation("Owner2");
		m.put("group2", "",colVis, "");
		m.put("group3", "",colVis, "");
		writer.addMutation(m);
		
		writer.close();		
	}
	
	/**
	 * Erases and re-creates the amino_hypothesis table
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	public static void resetHypothesisTable() throws AccumuloException, AccumuloSecurityException, TableNotFoundException{
		deleteAndCreateTable(hypothesisTable);
		
		// Insert data
		final BatchWriter writer = createBatchWriter(hypothesisTable);
		Mutation m = new Mutation(testOwnerPrefix + "1");
		m.put("HypothesisID1", "bucket",colVis, "bucketID1");
		m.put("HypothesisID1", "canEdit",colVis, "[]");
		m.put("HypothesisID1", "canView",colVis, "[]");
		m.put("HypothesisID1", "datasource",colVis, "DataSource1");
		m.put("HypothesisID1", "features",colVis, "[{\"id\":\"FeatureId1\",\"featureMetadataId\":\"" + testRatioId + "\",\"type\":\"RATIO\",\"min\":123.0,\"max\":123.0,\"timestampFrom\":0,\"timestampTo\":0,\"visibility\":\"UNCLASSIFIED\",\"count\":0,\"uniqueness\":0.0,\"include\":false}]");
		m.put("HypothesisID1", "justification",colVis, "Justification1");
		m.put("HypothesisID1", "name",colVis, "Hypothesis Name 1");
		m.put("HypothesisID1", "visibility",colVis, "UNCLASSIFIED");
		
		m.put("HypothesisID2", "bucket",colVis, "bucketID2");
		m.put("HypothesisID2", "canEdit",colVis, "[]");
		m.put("HypothesisID2", "canView",colVis, "[]");
		m.put("HypothesisID2", "datasource",colVis, "DataSource2");
		m.put("HypothesisID2", "features",colVis, "[{\"id\":\"FeatureId2\",\"featureMetadataId\":\"" + testNominalId + "\",\"type\":\"NOMINAL\",\"value\":\"even\",\"min\":0.0,\"max\":0.0,\"timestampFrom\":0,\"timestampTo\":0,\"visibility\":\"UNCLASSIFIED\",\"count\":0,\"uniqueness\":0.0,\"include\":false}]");
		m.put("HypothesisID2", "justification",colVis, "Justification 2");
		m.put("HypothesisID2", "name",colVis, "Hypothesis Name 2");
		m.put("HypothesisID2", "visibility",colVis, "UNCLASSIFIED");
		writer.addMutation(m);
		
		m = new Mutation(testOwnerPrefix + "2");
		m.put("HypothesisID3", "bucket",colVis, "bucketID3");
		m.put("HypothesisID3", "canEdit",colVis, "[]");
		m.put("HypothesisID3", "canView",colVis, "[]");
		m.put("HypothesisID3", "datasource",colVis, "DataSource3");
		m.put("HypothesisID3", "features",colVis, "[{\"id\":\"FeatureId3\",\"featureMetadataId\":\"" + testNominalId + "\",\"type\":\"NOMINAL\",\"value\":\"even\",\"min\":0.0,\"max\":0.0,\"timestampFrom\":0,\"timestampTo\":0,\"visibility\":\"UNCLASSIFIED\",\"count\":0,\"uniqueness\":0.0,\"include\":false}]");
		m.put("HypothesisID3", "justification",colVis, "Justification 3");
		m.put("HypothesisID3", "name",colVis, "Hypothesis Name 3");
		m.put("HypothesisID3", "visibility",colVis, "UNCLASSIFIED");
		writer.addMutation(m);
		
		writer.close();		
	}
	
	public static void resetMetadataTable() throws AccumuloException, AccumuloSecurityException, TableNotFoundException{
		deleteAndCreateTable(metadataTable);
		
		// Insert data
		final BatchWriter writer = createBatchWriter(metadataTable);
		Mutation m = new Mutation(testDatasourceId);
		m.put("bucket", "bucketID1",colVis, "{\"name\":\"TestBucket1\",\"displayName\":\"TestBucket1\",\"visibility\":\"UNCLASSIFIED\",\"domainIdName\":\"Test Domain Description\"}");
		m.put("bucket", "bucketID2",colVis, "{\"name\":\"TestBucket2\",\"displayName\":\"TestBucket2\",\"visibility\":\"UNCLASSIFIED\",\"domainIdName\":\"Test Domain Description\"}");
		m.put("feature", testRestrictionId, "{\"id\":\"" + testRestrictionId +"\",\"name\":\"Restrict\",\"visibility\":\"UNCLASSIFIED\",\"api_version\":\"1.3.0-SNAPSHOT\",\"job_version\":\"0.2\",\"description\":\"Restrict value to\",\"namespace\":\"Public\",\"type\":\"RESTRICTION\"}");
		//m.put("feature", testNominalId, "");
		//m.put("feature", testRatioId, "");
		writer.addMutation(m);
		
		m = new Mutation("hashcount");
		m.put("", "", colVis, "2");
		writer.addMutation(m);
		
		m = new Mutation("shardcount");
		m.put("", "", colVis, "14");
		writer.addMutation(m);

		writer.close();
	}

	
	///////////////////////////////////////////
	/*
	13 13:05:51,616 [shell.Shell] INFO : Attempting to begin bringing amino_bitmap_bitLookup_numbers online
	1186613585 even:0 [U]    x\xED!P\x00\x00\x00\x04\x00\x00\x00\x02\x03#n"\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\xA3\xFA\xE6\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x02
	1186613585 even:1 [U]    \x17\x889\xF8\x00\x00\x00\x04\x00\x00\x00\x02\x00\xA35x\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x02\x00\x19\x0CT\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02
	1186613585 even:number2:COUNT [U]    2
	1186613585 even:number2:FIRST [U]    2
	1186613585 even:number2:LAST [U]    4
	1186613585 even:number:COUNT [U]    2
	1186613585 even:number:FIRST [U]    2
	1186613585 even:number:LAST [U]    4
	1186613585 odd:0 [U]    d\xE9\x97\xD5\x00\x00\x00\x04\x00\x00\x00\x02\x02g 8\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x00\x02\x00\xC0,\x84\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x02
	1186613585 odd:1 [U]    *Z\x99\x96\x00\x00\x00\x04\x00\x00\x00\x02\x00\xD5\xB6X\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00}\x1Er\x00\x00\x00\x00\x00 \x00\x00\x00\x00\x00\x02
	1186613585 odd:number2:COUNT [U]    2
	1186613585 odd:number2:FIRST [U]    1
	1186613585 odd:number2:LAST [U]    3
	1186613585 odd:number:COUNT [U]    2
	1186613585 odd:number:FIRST [U]    1
	1186613585 odd:number:LAST [U]    3
	1484383801 1:0 [U]    y\xEB\xF3\x12\x00\x00\x00\x04\x00\x00\x00\x02\x02\x8B\xC7\xDC\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x02\x01C\x97\xBA\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x02
	1484383801 1:1 [U]    9\xE5!\xB7\x00\x00\x00\x04\x00\x00\x00\x02\x00<\x97\x16\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x02\x01\x92\x91\xF4\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02
	1484383801 1:number2:COUNT [U]    1
	1484383801 1:number2:FIRST [U]    1
	1484383801 1:number2:LAST [U]    1
	1484383801 1:number:COUNT [U]    1
	1484383801 1:number:FIRST [U]    1
	1484383801 1:number:LAST [U]    1
	1484383801 2:0 [U]    JB\x8C\x8B\x00\x00\x00\x04\x00\x00\x00\x02\x01c\xE7\xDE\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\xEE,\x84\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x02
	1484383801 2:1 [U]    9\xEE\x04\x8F\x00\x00\x00\x04\x00\x00\x00\x02\x001r\xF2\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x02\x01\x9D\xFD0\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x02
	1484383801 2:number2:COUNT [U]    1
	1484383801 2:number2:FIRST [U]    2
	1484383801 2:number2:LAST [U]    2
	1484383801 2:number:COUNT [U]    1
	1484383801 2:number:FIRST [U]    2
	1484383801 2:number:LAST [U]    2
	1484383801 3:0 [U]    s\xCF\xD4C\x00\x00\x00\x04\x00\x00\x00\x02\x00\xD8\xDD\xD6\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x02\x02\xC5\xA0\xCA\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x02
	1484383801 3:1 [U]    U\xB7,$\x00\x00\x00\x04\x00\x00\x00\x02\x01\xF4\x93\xF4\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x02\x00\xB9%j\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x02
	1484383801 3:number2:COUNT [U]    1
	1484383801 3:number2:FIRST [U]    3
	1484383801 3:number2:LAST [U]    3
	1484383801 3:number:COUNT [U]    1
	1484383801 3:number:FIRST [U]    3
	1484383801 3:number:LAST [U]    3
	1484383801 4:0 [U]    P)\x8E\x7F\x00\x00\x00\x04\x00\x00\x00\x02\x01\xC1\xE5V\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x02\x00\xBFg\x1A@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02
	1484383801 4:1 [U]    >\xA9\xDFi\x00\x00\x00\x04\x00\x00\x00\x02\x005\xA1|\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x02\x01\xBF\xAD|\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x02
	1484383801 4:number2:COUNT [U]    1
	1484383801 4:number2:FIRST [U]    4
	1484383801 4:number2:LAST [U]    4
	1484383801 4:number:COUNT [U]    1
	1484383801 4:number:FIRST [U]    4
	1484383801 4:number:LAST [U]    4
	1668757391 100000001.00000000:0 [U]    j\xD6\xC7u\x00\x00\x00\x04\x00\x00\x00\x02\x01=\x86\x02\x00\x00\x00\x00\x00\x00\x00 \x00\x00\x00\x02\x02\x1906\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02
	1668757391 100000001.00000000:1 [U]    .,\xD7\x1C\x00\x00\x00\x04\x00\x00\x00\x02\x00S\x84\xBA\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01\x1D\xE1\xFC\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x02
	1668757391 100000001.00000000:number2:COUNT [U]    1
	1668757391 100000001.00000000:number2:FIRST [U]    1
	1668757391 100000001.00000000:number2:LAST [U]    1
	1668757391 100000001.00000000:number:COUNT [U]    1
	1668757391 100000001.00000000:number:FIRST [U]    1
	1668757391 100000001.00000000:number:LAST [U]    1
	1668757391 100000002.00000000:0 [U]    -\xDD\xEF.\x00\x00\x00\x04\x00\x00\x00\x02\x00\xB2P\xA0\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\xBC\x9E\xD6\x00\x00 \x00\x00\x00\x00\x00\x00\x00\x00\x02
	1668757391 100000002.00000000:1 [U]    o\xDA\x98\x88\x00\x00\x00\x04\x00\x00\x00\x02\x00[\x8F\xBC\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x03#E\x06\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x02
	1668757391 100000002.00000000:number2:COUNT [U]    1
	1668757391 100000002.00000000:number2:FIRST [U]    2
	1668757391 100000002.00000000:number2:LAST [U]    2
	1668757391 100000002.00000000:number:COUNT [U]    1
	1668757391 100000002.00000000:number:FIRST [U]    2
	1668757391 100000002.00000000:number:LAST [U]    2
	1668757391 100000003.00000000:0 [U]    .?\xD9+\x00\x00\x00\x04\x00\x00\x00\x02\x01_\x898\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x02\x00\x12u\x8E\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02
	1668757391 100000003.00000000:1 [U]    Z;uU\x00\x00\x00\x04\x00\x00\x00\x02\x00\xFA\xBEP\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x01\xD7\x1DX\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x02
	1668757391 100000003.00000000:number2:COUNT [U]    1
	1668757391 100000003.00000000:number2:FIRST [U]    3
	1668757391 100000003.00000000:number2:LAST [U]    3
	1668757391 100000003.00000000:number:COUNT [U]    1
	1668757391 100000003.00000000:number:FIRST [U]    3
	1668757391 100000003.00000000:number:LAST [U]    3
	1668757391 100000004.00000000:0 [U]    ?`+$\x00\x00\x00\x04\x00\x00\x00\x02\x01bk\x0A\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x98\x96L\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x02
	-------------------------------------------------------------------------------------------------------------------- hit any key to continue or 'q' to quit --------------------------------------------------------------------------------------------------------------------
	1668757391 100000004.00000000:1 [U]    \x7F\xCF\xD1-\x00\x00\x00\x04\x00\x00\x00\x02\x023\xAA\x18\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x02\x01\xCA\xD4n\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x02
	1668757391 100000004.00000000:number2:COUNT [U]    1
	1668757391 100000004.00000000:number2:FIRST [U]    4
	1668757391 100000004.00000000:number2:LAST [U]    4
	1668757391 100000004.00000000:number:COUNT [U]    1
	1668757391 100000004.00000000:number:FIRST [U]    4
	1668757391 100000004.00000000:number:LAST [U]    4
	24124505 100000001.00000000:0 [U]    Y\x02V\xC4\x00\x00\x00\x04\x00\x00\x00\x02\x00\x19ml\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x02\xAE\xA5H\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x02
	24124505 100000001.00000000:1 [U]    PK\x19\x03\x00\x00\x00\x04\x00\x00\x00\x02\x00\xA2\x13\xAA\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01\xE0E\x1C\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x02
	24124505 100000001.00000000:number2:COUNT [U]    1
	24124505 100000001.00000000:number2:FIRST [U]    1
	24124505 100000001.00000000:number2:LAST [U]    1
	24124505 100000001.00000000:number:COUNT [U]    1
	24124505 100000001.00000000:number:FIRST [U]    1
	24124505 100000001.00000000:number:LAST [U]    1
	24124505 100000002.00000000:0 [U]    .\xD0\xA26\x00\x00\x00\x04\x00\x00\x00\x02\x01Y\xDC\x90\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x02\x00\x1C\xA8~\x00 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x02
	24124505 100000002.00000000:1 [U]    =~j\x0D\x00\x00\x00\x04\x00\x00\x00\x02\x00,\xAD\x16\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x02\x01\xBFF8\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x02
	24124505 100000002.00000000:number2:COUNT [U]    1
	24124505 100000002.00000000:number2:FIRST [U]    2
	24124505 100000002.00000000:number2:LAST [U]    2
	24124505 100000002.00000000:number:COUNT [U]    1
	24124505 100000002.00000000:number:FIRST [U]    2
	24124505 100000002.00000000:number:LAST [U]    2
	24124505 100000003.00000000:0 [U]    qv\xF3\xA4\x00\x00\x00\x04\x00\x00\x00\x02\x03X\x08\xCC\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x02\x003\xAE\xCE\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x02
	24124505 100000003.00000000:1 [U]    G\xC1F\xC2\x00\x00\x00\x04\x00\x00\x00\x02\x00\x80\xCFH\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01\xBD:\xEC\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x02
	24124505 100000003.00000000:number2:COUNT [U]    1
	24124505 100000003.00000000:number2:FIRST [U]    3
	24124505 100000003.00000000:number2:LAST [U]    3
	24124505 100000003.00000000:number:COUNT [U]    1
	24124505 100000003.00000000:number:FIRST [U]    3
	24124505 100000003.00000000:number:LAST [U]    3
	24124505 100000004.00000000:0 [U]    aG\xF4\xA8\x00\x00\x00\x04\x00\x00\x00\x02\x01\xE4\xC1|\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01%~&\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x02
	24124505 100000004.00000000:1 [U]    z\xE13\xB8\x00\x00\x00\x04\x00\x00\x00\x02\x01\xC5\x8F\xEA\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x02\x02\x11y\xB0\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02
	24124505 100000004.00000000:number2:COUNT [U]    1
	24124505 100000004.00000000:number2:FIRST [U]    4
	24124505 100000004.00000000:number2:LAST [U]    4
	24124505 100000004.00000000:number:COUNT [U]    1
	24124505 100000004.00000000:number:FIRST [U]    4
	24124505 100000004.00000000:number:LAST [U]    4
	628381357 100000001.00000000:0 [U]    i\x1B\xD7\xD3\x00\x00\x00\x04\x00\x00\x00\x02\x02\x15r\x0A\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x02\x013l\xB2\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x02
	628381357 100000001.00000000:1 [U]    Io\x1F\x99\x00\x00\x00\x04\x00\x00\x00\x02\x01\xF8fL\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00S\x12\xAE\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x02
	628381357 100000001.00000000:number2:COUNT [U]    1
	628381357 100000001.00000000:number2:FIRST [U]    1
	628381357 100000001.00000000:number2:LAST [U]    1
	628381357 100000001.00000000:number:COUNT [U]    1
	628381357 100000001.00000000:number:FIRST [U]    1
	628381357 100000001.00000000:number:LAST [U]    1
	628381357 100000002.00000000:0 [U]    u\x81I)\x00\x00\x00\x04\x00\x00\x00\x02\x02\x91\\"\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01\x1A\xAE$\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x02
	628381357 100000002.00000000:1 [U]    1\x18\x92S\x00\x00\x00\x04\x00\x00\x00\x02\x00N\xB50\x00\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x02\x01:\x0F`\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x02
	628381357 100000002.00000000:number2:COUNT [U]    1
	628381357 100000002.00000000:number2:FIRST [U]    2
	628381357 100000002.00000000:number2:LAST [U]    2
	628381357 100000002.00000000:number:COUNT [U]    1
	628381357 100000002.00000000:number:FIRST [U]    2
	628381357 100000002.00000000:number:LAST [U]    2
	628381357 100000003.00000000:0 [U]    }\x12j\xD2\x00\x00\x00\x04\x00\x00\x00\x02\x02`\x90z\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01\x88\x02\xDA\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x02
	628381357 100000003.00000000:1 [U]    F\x0F\xF4y\x00\x00\x00\x04\x00\x00\x00\x02\x01\xCC\xDD\xCE\x00\x00\x00\x00\x00 \x00\x00\x00\x00\x00\x02\x00c\xA1\xD2\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02
	628381357 100000003.00000000:number2:COUNT [U]    1
	628381357 100000003.00000000:number2:FIRST [U]    3
	628381357 100000003.00000000:number2:LAST [U]    3
	628381357 100000003.00000000:number:COUNT [U]    1
	628381357 100000003.00000000:number:FIRST [U]    3
	628381357 100000003.00000000:number:LAST [U]    3
	628381357 100000004.00000000:0 [U]    8U\xF8F\x00\x00\x00\x04\x00\x00\x00\x02\x01[8\x82\x00\x00\x00\x00 \x00\x00\x00\x00\x00\x00\x02\x00gw>\x00\x00\x00\x00\x00\x00\x00 \x00\x00\x00\x02
	628381357 100000004.00000000:1 [U]    \x19H\xC5E\x00\x00\x00\x04\x00\x00\x00\x02\x00u6\x1A\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x02\x00U\x10\x0E\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x02
	628381357 100000004.00000000:number2:COUNT [U]    1
	628381357 100000004.00000000:number2:FIRST [U]    4
	628381357 100000004.00000000:number2:LAST [U]    4
	628381357 100000004.00000000:number:COUNT [U]    1
	628381357 100000004.00000000:number:FIRST [U]    4
	628381357 100000004.00000000:number:LAST [U]    4
	
	13 13:05:51,778 [shell.Shell] INFO : Attempting to begin bringing amino_bitmap_byBucket_numbers online
	10:Number Domain:number2 4:0 [U]    x\xED!P\x00\x00\x00\x0A\x00\x00\x00\x02\x01\xC2\xAF\xC2\x00\x00\x00\x00\x00\x00\x00 \x00\x00\x00\x02\x00"\x11\xB8\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x16?\xDA\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x02\x00\x86K\x18@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01F\x1C\x96\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x08
	10:Number Domain:number2 4:1 [U]    FuC\x03\x00\x00\x00\x0A\x00\x00\x00\x02\x00\xA35x\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x02\x00'\x10\xB0\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x02\x00\xFBI\xBE\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x02\x00/\xBF\x0E\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00>[\x1C\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x08
	13:Number Domain:number 4:0 [U]    dm\xC4{\x00\x00\x00\x0A\x00\x00\x00\x02\x01[8\x82\x00\x00\x00\x00 \x00\x00\x00\x00\x00\x00\x02\x00\x072\x86\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00_zJ\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x02\x01HZL\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x02\x00\x19.|\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08
	13:Number Domain:number 4:1 [U]    \x7F\xCF\xD1-\x00\x00\x00\x0A\x00\x00\x00\x02\x005\xA1|\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x02\x00?\x94\x9C\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x02\x00G\x0B\xB2\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x03\x1A\xC7\xCC\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00't\xEA\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x08
	1:Number Domain:number 1:0 [U]    Y\x02V\xC4\x00\x00\x00\x0A\x00\x00\x00\x02\x01=\x86\x02\x00\x00\x00\x00\x00\x00\x00 \x00\x00\x00\x02\x00\xD7\xEC\x06\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x02\x00Q\xAE,\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x00\x02\x00$\xA7\xA2\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x02\x00<J\xD8\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x08
	1:Number Domain:number 1:1 [U]    Io\x1F\x99\x00\x00\x00\x0A\x00\x00\x00\x02\x00<\x97\x16\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x02\x00e|\x92\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\xB0\xC1 \x00\x00\x00\x00\x00 \x00\x00\x00\x00\x00\x02\x00\x1E\x91\xEA\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x02\x00\xDA\x12B\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x08
	4:Number Domain:number 3:0 [U]    s\xCF\xD4C\x00\x00\x00\x0A\x00\x00\x00\x02\x01_\x898\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x02\x01\x01\x07@\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x06\x8F\xBC\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x00\x02\x01$\x97b\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x02\x00\x12\xC7\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x08
	4:Number Domain:number 3:1 [U]    U\xB7,$\x00\x00\x00\x0A\x00\x00\x00\x02\x00\xFA\xBEP\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00X\x16z\x00\x00\x00\x00\x00 \x00\x00\x00\x00\x00\x02\x00z\x09\x00\x00\x00\x00\x00\x00 \x00\x00\x00\x00\x00\x02\x00q,f\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x02\x00o\xAF(\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08
	5:Number Domain:number2 1:0 [U]    y\xEB\xF3\x12\x00\x00\x00\x0A\x00\x00\x00\x02\x00\x19ml\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x03\x0D\xDFP\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x02\x00!\x91\xFE\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x02\x00\x0D\xD7z\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00x\xA9\\\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x08
	5:Number Domain:number2 1:1 [U]    PK\x19\x03\x00\x00\x00\x0A\x00\x00\x00\x02\x00S\x84\xBA\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x821\x9C\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\xF9r\xB2\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00)=>\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x89\xF2z\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x08
	5:Number Domain:number2 2:0 [U]    x\xED!P\x00\x00\x00\x0A\x00\x00\x00\x02\x00\xB2P\xA0\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\xA7\x8B\xEE\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x02\x00\xF87\xD2\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x02\x00?G\xBC\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02\x016\x0C\xE6\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x08
	5:Number Domain:number2 2:1 [U]    o\xDA\x98\x88\x00\x00\x00\x0A\x00\x00\x00\x02\x00N\xB50\x00\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x02\x00T\x80F\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x02\x01,:\xAA\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x02\x00\x1C\x83*\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x02\x01\x92\xE1r\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x08
	6:Number Domain:number 2:0 [U]    u\x81I)\x00\x00\x00\x0A\x00\x00\x00\x02\x01c\xE7\xDE\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x0B\x07\x98\x00\x00 \x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x07\x95\x96\x00 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01\xAC\xE9\x10\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x88\x9C$\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x08
	6:Number Domain:number 2:1 [U]    1\x18\x92S\x00\x00\x00\x0A\x00\x00\x00\x02\x00,\xAD\x16\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x02\x00\x04\xC5\xDA\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x02\x00*\x1C\xC8\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00`\xB2\x10\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\xCC\x82\xC2\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x08
	7:Number Domain:number2 3:0 [U]    }\x12j\xD2\x00\x00\x00\x0A\x00\x00\x00\x02\x00\xD8\xDD\xD6\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x02\x00\x99 \xF0\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01\xB5M\xF4\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x02\x000\xBC\x0C\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x02\x00\x90\x8A\x88\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x08
	7:Number Domain:number2 3:1 [U]    Z;uU\x00\x00\x00\x0A\x00\x00\x00\x02\x00\x80\xCFH\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00T\xE7\x0E\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01\x1E\xDD\x9A\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x02\x00;\xEB\xAC\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\xA1\\\x06\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x08
	
	13 13:05:51,806 [shell.Shell] INFO : Attempting to begin bringing amino_feature_lookup_numbers online
	1016606636 0:number2 [U]    24124505:100000004.00000000
	1031694860 1:number2 [U]    24124505:100000002.00000000
	103702090 1:number [U]    1484383801:2
	1049788068 1:number2 [U]    1484383801:3
	1051320168 1:number2 [U]    1484383801:4
	1057802665 1:number2 [U]    628381357:100000001.00000000
	1063267107 0:number2 [U]    1668757391:100000004.00000000
	1118716231 0:number [U]    628381357:100000001.00000000
	112471948 1:number [U]    1484383801:4
	1175450744 1:number2 [U]    628381357:100000003.00000000
	1182089986 1:number2 [U]    1668757391:100000004.00000000
	1203848897 1:number [U]    24124505:100000003.00000000
	1232019352 1:number [U]    628381357:100000001.00000000
	1245875338 0:number2 [U]    1484383801:2
	127066816 1:number [U]    1484383801:1
	1276252027 0:number [U]    628381357:100000003.00000000
	1290012439 0:number [U]    1186613585:odd
	1344900734 0:number2 [U]    1484383801:4
	1347098882 1:number2 [U]    24124505:100000001.00000000
	1366883217 0:number [U]    1484383801:1
	1378583658 0:number2 [U]    628381357:100000002.00000000
	1438067747 1:number [U]    1484383801:3
	1493325507 0:number [U]    24124505:100000001.00000000
	1513846100 1:number2 [U]    1668757391:100000003.00000000
	1632105639 0:number [U]    24124505:100000004.00000000
	165062175 1:number2 [U]    628381357:100000002.00000000
	1684915322 0:number [U]    1186613585:even
	1693030356 0:number2 [U]    1186613585:odd
	175150956 1:number2 [U]    1668757391:100000001.00000000
	1763432402 0:number2 [U]    628381357:100000001.00000000
	1792460660 0:number2 [U]    1668757391:100000001.00000000
	1795234190 0:number2 [U]    24124505:100000003.00000000
	1876596871 1:number2 [U]    1668757391:100000002.00000000
	1903621027 0:number [U]    24124505:100000003.00000000
	192018364 1:number [U]    1668757391:100000002.00000000
	1943000130 0:number [U]    1484383801:3
	1971407144 0:number [U]    628381357:100000002.00000000
	2028806479 0:number2 [U]    1186613585:even
	2045506321 0:number2 [U]    1484383801:1
	2061579191 1:number [U]    24124505:100000004.00000000
	2098358993 0:number2 [U]    628381357:100000003.00000000
	2144325932 1:number [U]    1668757391:100000004.00000000
	245809988 1:number [U]    628381357:100000004.00000000
	270133545 1:number2 [U]    24124505:100000003.00000000
	339899769 1:number [U]    24124505:100000001.00000000
	342273794 1:number2 [U]    1186613585:even
	373953587 0:number2 [U]    1668757391:100000002.00000000
	394803703 1:number [U]    1186613585:even
	424199492 1:number2 [U]    628381357:100000004.00000000
	448187185 1:number2 [U]    1186613585:odd
	454802119 0:number2 [U]    1484383801:3
	525847072 1:number [U]    1668757391:100000003.00000000
	53325240 0:number2 [U]    24124505:100000001.00000000
	665894981 0:number [U]    1668757391:100000001.00000000
	710580629 1:number [U]    1186613585:odd
	725324304 0:number2 [U]    24124505:100000002.00000000
	728174685 0:number [U]    628381357:100000004.00000000
	737224454 0:number [U]    1668757391:100000003.00000000
	743268720 0:number [U]    1668757391:100000004.00000000
	746388465 0:number [U]    1484383801:2
	769519405 0:number [U]    1668757391:100000002.00000000
	774690587 1:number [U]    1668757391:100000001.00000000
	775936298 0:number2 [U]    1668757391:100000003.00000000
	785424949 0:number [U]    24124505:100000002.00000000
	823693906 1:number [U]    628381357:100000002.00000000
	93692612 1:number [U]    24124505:100000002.00000000
	943499992 0:number [U]    1484383801:4
	945158213 0:number2 [U]    628381357:100000004.00000000
	951188815 1:number2 [U]    24124505:100000004.00000000
	966506965 1:number [U]    628381357:100000003.00000000
	971317686 1:number2 [U]    1484383801:1
	971900046 1:number2 [U]    1484383801:2
	
	13 13:05:51,824 [shell.Shell] INFO : Attempting to begin bringing amino_group_hypothesisLUT_numbers online
	public TestUser1:c1fbda1c-d303-4879-9baa-43e199f9024d [U]
	public TestUser1:e56b48b6-2d5f-4c3c-b7cc-cfa8123095c8 [U]
	public TestUser2:5fe7ed38-cdbc-47b9-bc32-ab093e8fb724 [U]
	
	13 13:05:51,866 [shell.Shell] INFO : Attempting to begin bringing amino_hypothesis_numbers online
	TestUser1 c1fbda1c-d303-4879-9baa-43e199f9024d:bucket [U]    1025254987
	TestUser1 c1fbda1c-d303-4879-9baa-43e199f9024d:canEdit [U]    ["public"]
	TestUser1 c1fbda1c-d303-4879-9baa-43e199f9024d:canView [U]    ["public"]
	TestUser1 c1fbda1c-d303-4879-9baa-43e199f9024d:datasource [U]    Number Domain
	TestUser1 c1fbda1c-d303-4879-9baa-43e199f9024d:features [U]    [{"id":"43133807-9e28-4917-84dd-af0b2e741425","featureMetadataId":"24124505","type":"RATIO","min":1.0,"max":2.0,"timestampFrom":0,"timestampTo":0,"visibility":"UNCLASSIFIED","count":0,"uniqueness":0.0,"include":false}]
	TestUser1 c1fbda1c-d303-4879-9baa-43e199f9024d:justification [U]    testing
	TestUser1 c1fbda1c-d303-4879-9baa-43e199f9024d:name [U]    TestUser1 - Ratio
	TestUser1 c1fbda1c-d303-4879-9baa-43e199f9024d:visibility [U]    UNCLASSIFIED
	TestUser1 e56b48b6-2d5f-4c3c-b7cc-cfa8123095c8:bucket [U]    1025254987
	TestUser1 e56b48b6-2d5f-4c3c-b7cc-cfa8123095c8:canEdit [U]    ["public"]
	TestUser1 e56b48b6-2d5f-4c3c-b7cc-cfa8123095c8:canView [U]    ["public"]
	TestUser1 e56b48b6-2d5f-4c3c-b7cc-cfa8123095c8:datasource [U]    Number Domain
	TestUser1 e56b48b6-2d5f-4c3c-b7cc-cfa8123095c8:features [U]    [{"id":"a9d882f7-eb70-4340-bab8-25b41b8b4cff","featureMetadataId":"1484383801","type":"NOMINAL","value":"1","min":0.0,"max":0.0,"timestampFrom":0,"timestampTo":0,"visibility":"UNCLASSIFIED","count":0,"uniqueness":0.0,"include":false}]
	TestUser1 e56b48b6-2d5f-4c3c-b7cc-cfa8123095c8:justification [U]    testing
	TestUser1 e56b48b6-2d5f-4c3c-b7cc-cfa8123095c8:name [U]    Hypothesis 1 - Nominal
	TestUser1 e56b48b6-2d5f-4c3c-b7cc-cfa8123095c8:visibility [U]    UNCLASSIFIED
	TestUser2 5fe7ed38-cdbc-47b9-bc32-ab093e8fb724:bucket [U]    1025254987
	TestUser2 5fe7ed38-cdbc-47b9-bc32-ab093e8fb724:canEdit [U]    ["public"]
	TestUser2 5fe7ed38-cdbc-47b9-bc32-ab093e8fb724:canView [U]    ["public"]
	TestUser2 5fe7ed38-cdbc-47b9-bc32-ab093e8fb724:datasource [U]    Number Domain
	TestUser2 5fe7ed38-cdbc-47b9-bc32-ab093e8fb724:features [U]    [{"id":"338f1ec9-fe8b-4d96-8709-458d5f7ce16f","featureMetadataId":"24124505","type":"RATIO","min":1.0,"max":2.0,"timestampFrom":0,"timestampTo":0,"visibility":"UNCLASSIFIED","count":0,"uniqueness":0.0,"include":false},{"id":"24b41a4a-1137-4cfb-9133-21a155a064d6","featureMetadataId":"1","type":"RESTRICTION","value":"[\\"1\\",\\"4\\"]","min":0.0,"max":0.0,"timestampFrom":0,"timestampTo":0,"visibility":"UNCLASSIFIED","count":0,"uniqueness":0.0,"include":false}]
	TestUser2 5fe7ed38-cdbc-47b9-bc32-ab093e8fb724:justification [U]    testing
	TestUser2 5fe7ed38-cdbc-47b9-bc32-ab093e8fb724:name [U]    TestUser2 - Ratio Restrict
	TestUser2 5fe7ed38-cdbc-47b9-bc32-ab093e8fb724:visibility [U]    UNCLASSIFIED
	
	13 13:05:51,893 [shell.Shell] INFO : Attempting to begin bringing amino_metadata_numbers online
	Number Domain bucket:1025254987 [U]    {"name":"number","displayName":"number","visibility":"UNCLASSIFIED","domainIdName":"Number Domain Description"}
	Number Domain bucket:838311868 [U]    {"name":"number2","displayName":"number2","visibility":"UNCLASSIFIED","domainIdName":"Number Domain Description"}
	Number Domain feature:1 [U]    {"id":"1","name":"Restrict","visibility":"UNCLASSIFIED","api_version":"1.3.0-SNAPSHOT","job_version":"0.2","description":"Restrict value to","namespace":"Public","type":"RESTRICTION"}
	Number Domain feature:1186613585 [U]    {"id":"1186613585","name":"Even or odd","visibility":"UNCLASSIFIED","api_version":"1.3.0-SNAPSHOT","job_version":"0.2","description":"Whether this feature is even or odd","namespace":"Public","type":"NOMINAL","allowedValues":["even","odd"],"featureFactCount":{"number2":2,"number":2},"bucketValueCount":{"number2":4,"number":4}}
	Number Domain feature:1484383801 [U]    {"id":"1484383801","name":"Has digit (Nominal)","visibility":"UNCLASSIFIED","api_version":"1.3.0-SNAPSHOT","job_version":"0.2","description":"Has this digit","namespace":"Public","type":"NOMINAL","allowedValues":["1","2","3","4"],"featureFactCount":{"number2":4,"number":4},"bucketValueCount":{"number2":4,"number":4}}
	Number Domain feature:1668757391 [U]    {"id":"1668757391","name":"First digit","visibility":"UNCLASSIFIED","api_version":"1.3.0-SNAPSHOT","job_version":"0.2","description":"First digit of the number","namespace":"Public","type":"RATIO","min":1.0,"max":4.0,"featureFactCount":{"number2":4,"number":4},"bucketValueCount":{"number2":4,"number":4}}
	Number Domain feature:24124505 [U]    {"id":"24124505","name":"Has digit (Ratio)","visibility":"UNCLASSIFIED","api_version":"1.3.0-SNAPSHOT","job_version":"0.2","description":"Has this digit","namespace":"Public","type":"RATIO","min":1.0,"max":4.0,"featureFactCount":{"number2":4,"number":4},"bucketValueCount":{"number2":4,"number":4}}
	Number Domain feature:628381357 [U]    {"id":"628381357","name":"Is Number","visibility":"UNCLASSIFIED","api_version":"1.3.0-SNAPSHOT","job_version":"0.2","description":"Is Number","namespace":"Public","type":"RATIO","min":1.0,"max":4.0,"featureFactCount":{"number2":4,"number":4},"bucketValueCount":{"number2":4,"number":4}}
	hashcount : [U]    2
	shardcount : [U]    14
	
	13 13:05:51,911 [shell.Shell] INFO : Attempting to begin bringing amino_query_result_numbers online
	TestUser1 9223370676314754116:hypothesis_at_runtime [U]    {"owner":"TestUser1","id":"c1fbda1c-d303-4879-9baa-43e199f9024d","name":"TestUser1 - Ratio","bucketid":"1025254987","canEdit":["public"],"canView":["public"],"datasourceid":"Number Domain","justification":"testing","visibility":"UNCLASSIFIED","hypothesisFeatures":[{"id":"43133807-9e28-4917-84dd-af0b2e741425","featureMetadataId":"24124505","type":"RATIO","min":1.0,"max":2.0,"timestampFrom":0,"timestampTo":0,"visibility":"UNCLASSIFIED","count":0,"uniqueness":0.0,"include":false}]}
	TestUser1 9223370676314754116:id [U]    c1fbda1c-d303-4879-9baa-43e199f9024d
	TestUser1 9223370676314754116:name [U]    TestUser1 - Ratio
	TestUser1 9223370676314754116:result_count [U]    2
	TestUser1 9223370676314754116:result_set [U]    [{"bucketName":"2"},{"bucketName":"1"}]
	TestUser1 9223370676314821814:hypothesis_at_runtime [U]    {"owner":"TestUser1","id":"e56b48b6-2d5f-4c3c-b7cc-cfa8123095c8","name":"Hypothesis 1 - Nominal","bucketid":"1025254987","canEdit":["public"],"canView":["public"],"datasourceid":"Number Domain","justification":"testing","visibility":"UNCLASSIFIED","hypothesisFeatures":[{"id":"a9d882f7-eb70-4340-bab8-25b41b8b4cff","featureMetadataId":"1484383801","type":"NOMINAL","value":"1","min":0.0,"max":0.0,"timestampFrom":0,"timestampTo":0,"visibility":"UNCLASSIFIED","count":0,"uniqueness":0.0,"include":false}]}
	TestUser1 9223370676314821814:id [U]    e56b48b6-2d5f-4c3c-b7cc-cfa8123095c8
	TestUser1 9223370676314821814:name [U]    Hypothesis 1 - Nominal
	TestUser1 9223370676314821814:result_count [U]    1
	TestUser1 9223370676314821814:result_set [U]    [{"bucketName":"1"}]
	TestUser2 9223370676314622492:hypothesis_at_runtime [U]    {"owner":"TestUser2","id":"5fe7ed38-cdbc-47b9-bc32-ab093e8fb724","name":"TestUser2 - Ratio Restrict","bucketid":"1025254987","canEdit":["public"],"canView":["public"],"datasourceid":"Number Domain","justification":"testing","visibility":"UNCLASSIFIED","hypothesisFeatures":[{"id":"24b41a4a-1137-4cfb-9133-21a155a064d6","featureMetadataId":"1","type":"RESTRICTION","value":"[\\"1\\",\\"4\\"]","min":0.0,"max":0.0,"timestampFrom":0,"timestampTo":0,"visibility":"UNCLASSIFIED","count":0,"uniqueness":0.0,"include":false},{"id":"338f1ec9-fe8b-4d96-8709-458d5f7ce16f","featureMetadataId":"24124505","type":"RATIO","min":1.0,"max":2.0,"timestampFrom":0,"timestampTo":0,"visibility":"UNCLASSIFIED","count":0,"uniqueness":0.0,"include":false}]}
	TestUser2 9223370676314622492:id [U]    5fe7ed38-cdbc-47b9-bc32-ab093e8fb724
	TestUser2 9223370676314622492:name [U]    TestUser2 - Ratio Restrict
	TestUser2 9223370676314622492:result_count [U]    1
	TestUser2 9223370676314622492:result_set [U]    [{"bucketName":"1"}]


	*/
	
	
	
	
	
}
