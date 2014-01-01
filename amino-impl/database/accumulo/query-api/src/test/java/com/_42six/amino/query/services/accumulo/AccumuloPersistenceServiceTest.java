package com._42six.amino.query.services.accumulo;

import com._42six.amino.query.exception.BigTableException;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static org.junit.Assert.*;

public class AccumuloPersistenceServiceTest extends AccumuloTest {
	
	@Test
	public void testGetConnector(){
		 Connector connector = persistenceService.getConnector();
		 assertNotNull(connector);
	}
	
	@Test
    public void testCreateTable() throws BigTableException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
		persistenceService.createTable("TestTable");
		Connector con = persistenceService.getConnector();
		assertTrue(con.tableOperations().exists("TestTable"));
		con.tableOperations().delete("TestTable");
    }

	@Test
    public void testTableExists() {
		assertTrue(persistenceService.getConnector().tableOperations().exists(hypothesisTable));
		assertTrue(persistenceService.tableExists(hypothesisTable));
    }

	@Test
    public void testInsertRow() throws Exception {
		final Connector conn = persistenceService.getConnector();		
		persistenceService.insertRow("testInsertRow", "CF", "CQ", "U", "VALUE", testTable);
		final Scanner scan =conn.createScanner(testTable, auths);
		scan.setRange(new Range("testInsertRow"));
		scan.fetchColumn(new Text("CF"), new Text( "CQ"));
		for(Entry<Key, Value> e : scan){
			final Key key = e.getKey();
			assertEquals("testInsertRow", key.getRow().toString());
			assertEquals("CF", key.getColumnFamily().toString());
			assertEquals("CQ", key.getColumnQualifier().toString());
			assertEquals(new Text("U"), key.getColumnVisibility());
			assertEquals("VALUE", e.getValue().toString());
		}
    }

	@Test
    public void testInsertRows() throws Exception {
		deleteAndCreateTable(testTable);

		final Connector conn = persistenceService.getConnector();
		List<Mutation> rows = new ArrayList<Mutation>();
		rows.add(persistenceService.createInsertMutation("testInsertRows1", "CF1", "CQ1", "U", "Value1"));
		rows.add(persistenceService.createInsertMutation("testInsertRows2", "CF2", "CQ2", "U", "Value2"));
		persistenceService.insertRows(rows, testTable);
		
		final Scanner scan =conn.createScanner(testTable, auths);
		scan.setRange(new Range("testInsertRows1"));
		scan.fetchColumn(new Text("CF1"), new Text( "CQ1"));
		for(Entry<Key, Value> e : scan){
			final Key key = e.getKey();
			assertEquals("testInsertRows1", key.getRow().toString());
			assertEquals("CF1", key.getColumnFamily().toString());
			assertEquals("CQ1", key.getColumnQualifier().toString());
			assertEquals(new Text("U"), key.getColumnVisibility());
			assertEquals("Value1", e.getValue().toString());
		}
		
		scan.clearColumns();
		scan.setRange(new Range("testInsertRows2"));
		scan.fetchColumn(new Text("CF2"), new Text( "CQ2"));
		for(Entry<Key, Value> e : scan){
			final Key key = e.getKey();
			assertEquals("testInsertRows2", key.getRow().toString());
			assertEquals("CF2", key.getColumnFamily().toString());
			assertEquals("CQ2", key.getColumnQualifier().toString());
			assertEquals(new Text("U"), key.getColumnVisibility());
			assertEquals("Value2", e.getValue().toString());
		}
    }

	@Ignore("Not Implemented Yet") @Test
    public void testDeleteCell() {
		
    }

	@Ignore("Not Implemented Yet") @Test
    public void testDeleteCells() {
		
    }

	@Ignore("Not Implemented Yet") @Test
    public void testCreateBatchWriter() {		
		
   }

	@Ignore("Not Implemented Yet") @Test
    public void testCreateScanner() {		
		
    }

	@Ignore("Not Implemented Yet") @Test
    public void testCreateBatchScanner() {		
		
    }

	@Ignore("Not Implemented Yet") @Test
	public void testCreateBatchDeleter() {		
		
	}

	@Ignore("Not Implemented Yet") @Test
    public void testGetLoggedInUserAuthorizations(){
		
    }

	@Ignore("Not Implemented Yet") @Test
	public void testCreateConfiguredScanner(){
		
	}
	
	@Ignore("Not Implemented Yet") @Test
	public void testCreateConfiguredBatchScanner(){
		
	}
	
	@Ignore("Not Implemented Yet") @Test
	public  void testConfigureBatchScanner(){
		
	}

	@Ignore("Not Implemented Yet") @Test
	public void testSetCustomIteratorOnScanner(){
		
	}

	@Ignore("Not Implemented Yet") @Test
	public  void testGenerateRanges(){
		
	}
}
