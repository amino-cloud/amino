package com._42six.amino.query.services.accumulo;

import com._42six.amino.common.bitmap.AminoBitmap;
import com._42six.amino.common.bitmap.BitmapUtils;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AccumuloQueryServiceTest extends AccumuloTest {
	
	private static final String INSTANCE_NAME = "";
	private static final String ZOOKEEPERS = "";
	private static final String ACCUMULO_USER = "";
	private static final String ACCUMULO_PW = "";

	static AccumuloQueryService queryService;
        static String[] perms = { "U" };
	
	@BeforeClass
	public static void setupClass(){
		queryService = new AccumuloQueryService(persistenceService, new AccumuloMetadataService(persistenceService));
	}

    @Test @Ignore
    public void testDateRange() throws TableNotFoundException, IOException {
        String featureId = "560526584";
        String startDate = "00000130487FE800";
        String endDate = "0000013357468800";

        Authorizations auths = new Authorizations(perms);
        persistenceService = new AccumuloPersistenceService(INSTANCE_NAME, ZOOKEEPERS, ACCUMULO_USER, ACCUMULO_PW, false);
        final Scanner bitLookupScanner = persistenceService.createScanner(bitLookupTable + "_sloop", auths);

        AccumuloScanConfig conf = new AccumuloScanConfig();
        conf.setStartRow(featureId);
        conf.setStartColumnFamily(startDate);
        conf.setStartColumnQualifier("0");
        conf.setEndRow(featureId);
        conf.setEndColumnFamily(endDate);
        conf.setEndColumnQualifier("~~~~~~~~~~~");

        System.out.println("Set up config. Waiting to call setRange");

        bitLookupScanner.setRange(persistenceService.createRangeForConfig(conf));
        
        System.out.println("Set the range");

        Iterator<Map.Entry<Key, Value>> itr = bitLookupScanner.iterator();
        System.out.println("Got the iterators");
        int i = 0;
        while (itr.hasNext()) {
            Map.Entry<Key, Value> entry = itr.next();
            System.out.println("Key: " + entry.getKey().toString() + " Value: " + entry.getValue().toString());
            i++;
        }
        System.out.println("Count: " + i);

    }

    @Test @Ignore
    public void testBits() throws TableNotFoundException, IOException {
        Authorizations auths = new Authorizations(perms);
        persistenceService = new AccumuloPersistenceService(INSTANCE_NAME, ZOOKEEPERS, ACCUMULO_USER, ACCUMULO_PW, false);
        final Scanner bitLookupScanner = persistenceService.createScanner(bitLookupTable + "_numbers", auths);
        bitLookupScanner.setRange(new Range("24124505"));
        bitLookupScanner.fetchColumn(new Text("100000007.00000000"), new Text("0"));
        bitLookupScanner.fetchColumn(new Text("100000008.00000000"), new Text("0"));
        bitLookupScanner.fetchColumn(new Text("100000009.00000000"), new Text("0"));

        Iterator<Map.Entry<Key, Value>> itr = bitLookupScanner.iterator();
        while(itr.hasNext()){
            Map.Entry<Key, Value> entry = itr.next();
            AminoBitmap bitmap = BitmapUtils.fromValue(entry.getValue());
            System.out.println("value " + entry.getKey().getColumnFamily().toString() + " has bits: " + bitmap.toString());
        }


        bitLookupScanner.setRange(new Range("1484383801"));
        bitLookupScanner.fetchColumn(new Text("4"), new Text("0"));

        itr = bitLookupScanner.iterator();
        while(itr.hasNext()){
            Map.Entry<Key, Value> entry = itr.next();
            AminoBitmap bitmap = BitmapUtils.fromValue(entry.getValue());
            System.out.println("value " + entry.getKey().getColumnFamily().toString() + " has bits: " + bitmap.toString());
        }

        final Scanner valueScanner = persistenceService.createScanner(byBucketTable + "_numbers", auths);
        bitLookupScanner.fetchColumn(new Text("78"), new Text("0"));

        itr = valueScanner.iterator();
        while(itr.hasNext()){
            Map.Entry<Key, Value> entry = itr.next();
            AminoBitmap bitmap = BitmapUtils.fromValue(entry.getValue());
            System.out.println("value " + entry.getKey().getColumnFamily().toString() + " has bits: " + bitmap.toString());
        }


        Scanner scan = persistenceService.createScanner("Fadsf", null);

        scan.setRange(new Range(new Key("a", "b", "1"), new Key("a", "b", "~~~~~~~")));

        System.out.println("Done");
    }

	@Ignore("Not implemented Yet") @Test
    public void listResults() {
 
    }

	@Ignore("Not implemented Yet") @Test
    public void getResult() {

    }

	@Ignore("Not implemented Yet") @Test
    public void createResult() {
 
    }

	@Ignore("Not implemented Yet") @Test
    public void deleteResult() {

    }

	@Ignore("Not implemented Yet") @Test
	public void findHypothesesByBucketValues(){
		
	}
	
	@Ignore("Not implemented Yet") @Test
	public void createNonPersistedHypothesisListForBucketValue() {
// ME
	}
	
	@Ignore("Not implemented Yet") @Test
	public void createNonPersistedHypothesisForBucketValue(){

    }
	
	@Ignore("Not implemented Yet") @Test
    public void getCountForHypothesisFeature() {

    }
	
	@Ignore("Not implemented Yet") @Test
	public void getUniqueness() {

	}

	@Test
	public void  isNumeric() {
		assertTrue(AccumuloQueryService.isNumeric("1234"));
		assertFalse(AccumuloQueryService.isNumeric("bad"));
		assertFalse(AccumuloQueryService.isNumeric("12bad4"));
	}
	
	@Ignore("Not implemented Yet") @Test
	public void getBitmaskScanInformationForQuery() {

	}	
}
