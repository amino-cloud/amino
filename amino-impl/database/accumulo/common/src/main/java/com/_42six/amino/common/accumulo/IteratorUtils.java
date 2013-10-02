package com._42six.amino.common.accumulo;

import com._42six.amino.common.FeatureFactType;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class IteratorUtils {

	public static int MAX_SPLITS = 1000;  // perhaps we should get this from config somehow?
	// or maybe just set it to number of shards if we go that way.
	public static final String TEMP_SUFFIX = "_temp";
	public static final String OLD_SUFFIX = "old";

    public static Range exactRow(String rowId, String cf){
        final Key key = new Key(rowId, cf);
        return new Range(key, key.followingKey(PartialKey.ROW_COLFAM));
    }

    public static Range exactRow(String rowId, String cf, String cq){
        final Key key = new Key(rowId, cf, cq);
        return new Range(key, key.followingKey(PartialKey.ROW_COLFAM_COLQUAL));
    }

	public static Connector connect(final String instanceName, final String zookeepers, final String user, final String password) throws IOException
	{
		final ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zookeepers);
        Connector connector;
		try
		{	
			connector = instance.getConnector(user, password.getBytes());
		} catch (AccumuloException e) {
			throw new IOException(e);
		} catch(AccumuloSecurityException e) {
			throw new IOException(e);
		}
        return connector;
	}
	public static boolean createTable(TableOperations tableOps, String tableName, boolean deleteIfExists, boolean markAsTemp) throws IOException
	{
		return createTable(tableOps, tableName, 1, deleteIfExists, markAsTemp);
	}

	public static boolean createTable(TableOperations tableOps, String tableName, SortedSet<Text> splits, boolean deleteIfExists, boolean markAsTemp) throws IOException
	{
		boolean success = true;
		tableName = getTableName(tableName, markAsTemp);

		if (isTableOperationNeeded(tableOps, tableName, deleteIfExists))
		{
			success = executeTableDeletion(tableOps, tableName);

			if (success) success = executeTableCreation(tableOps, tableName, splits);
		}
		return success;
	}

	public static boolean createTable(TableOperations tableOps, String tableName, int numShards, boolean deleteIfExists, boolean markAsTemp) throws IOException
	{
		boolean success = true;
		tableName = getTableName(tableName, markAsTemp);

		if (isTableOperationNeeded(tableOps, tableName, deleteIfExists))
		{
			success = executeTableDeletion(tableOps, tableName);
			if (success)
			{
				SortedSet<Text> sortedSplits = getDefaultSplits(numShards);
				success = executeTableCreation(tableOps, tableName, sortedSplits);
			}
		}
		return success;
	}

	private static String getTableName(String tableName, boolean markAsTemp)
	{
		if (markAsTemp) tableName = tableName + TEMP_SUFFIX;
		return tableName;
	}

	private static boolean isTableOperationNeeded(TableOperations tableOps, String tableName, boolean deleteIfExists)
	{
		if (!tableOps.exists(tableName) || (tableOps.exists(tableName) && deleteIfExists))
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	private static boolean executeTableDeletion(TableOperations tableOps, String tableName)
	{
		if( tableOps.exists(tableName) ){
			try {
				tableOps.delete(tableName);
				return true;
			} catch (AccumuloException e) {
				e.printStackTrace();
				return false;
			} catch (AccumuloSecurityException e) {
				e.printStackTrace();
				return false;
			} catch (TableNotFoundException e) {
				e.printStackTrace();
				return false;
			}
		}
		else
		{
			return true;
		}
	}

	private static boolean executeTableCreation(TableOperations tableOps, String tableName, SortedSet<Text> sortedSplits) throws IOException
	{
		try {
			if (sortedSplits == null)
			{
				tableOps.create(tableName);
			}
			else
			{
				tableOps.create(tableName);
				tableOps.addSplits(tableName, sortedSplits);
			}

			tableOps.setProperty(tableName,
					org.apache.accumulo.core.conf.Property.TABLE_LOAD_BALANCER.getKey(),
					org.apache.accumulo.server.master.balancer.TableLoadBalancer.class.getName());

			tableOps.flush(tableName, null, null, false); // so the splits get balanced. (not sure if a major compaction is required to do this)
			
			return true;

		} catch (AccumuloException e) {
			e.printStackTrace();
			return false;
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
			return false;
		} catch (TableExistsException e) {
			e.printStackTrace();
			return false;
		} catch (TableNotFoundException e) {
			throw new IOException(e);
		}
	}


	private static SortedSet<Text> getDefaultSplits(int numShards){
		TreeSet<Text> splits = new TreeSet<Text>();
		for( int ii = 1; ii < numShards; ii++){
			splits.add( new Text(Integer.toString(ii)+":"));
		}
		if( splits.isEmpty() ){
			splits = null;
		}
		return splits;
	}

	private static final Map<String,String> aggOptions = new HashMap<String,String>();
	private static final Map<String,String> bitAggOptions = new HashMap<String,String>();
	private static final Map<String,String> idxAggOptions = new HashMap<String,String>();

	static {
		String aggIterValue = "10,org.apache.accumulo.core.iterators.AggregatingIterator";
		aggOptions.put( "table.iterator.majc.agg", aggIterValue);
		aggOptions.put( "table.iterator.minc.agg", aggIterValue);
		aggOptions.put( "table.iterator.scan.agg", aggIterValue);
		String bitAggValue = "com._42six.amino.bitmap.iterators.Aggregator"; //Aggregator.class.getName(); ???
		String idxAggValue = "com._42six.amino.bitmap.iterators.IndexCountAggregator";
		bitAggOptions.put("table.iterator.majc.agg.opt.", bitAggValue);
		bitAggOptions.put("table.iterator.minc.agg.opt.", bitAggValue);
		bitAggOptions.put("table.iterator.scan.agg.opt.", bitAggValue);
		for( FeatureFactType type : FeatureFactType.values() ){
			String typeName = type.name();
			bitAggOptions.put("table.iterator.majc.agg.opt."+typeName, bitAggValue);
			bitAggOptions.put("table.iterator.minc.agg.opt."+typeName, bitAggValue);
			bitAggOptions.put("table.iterator.scan.agg.opt."+typeName, bitAggValue);

			idxAggOptions.put("table.iterator.majc.agg.opt."+typeName, idxAggValue);
			idxAggOptions.put("table.iterator.minc.agg.opt."+typeName, idxAggValue);
			idxAggOptions.put("table.iterator.scan.agg.opt."+typeName, idxAggValue);
		}
	}

	private static void addOptionsMap(TableOperations tableOps, String tableName, Map<String,String> optionsMap){
		for( Entry<String, String> entry : optionsMap.entrySet() ){
			try {
				tableOps.setProperty(tableName, entry.getKey(), entry.getValue());
			} catch (AccumuloException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AccumuloSecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void addAggregator(TableOperations tableOps, String tableName){
		addOptionsMap(tableOps, tableName, aggOptions);
	}

	public static void addBitmapAggregator(TableOperations tableOps, String tableName){
		addOptionsMap(tableOps, tableName, bitAggOptions);
	}

	public static void addIndexAggregator(TableOperations tableOps, String tableName){
		addOptionsMap(tableOps, tableName, idxAggOptions);
	}

	public static void compactTable(TableOperations tableOps, String tableName, boolean fireAndForget) throws IOException {
		try {
			String nowStr = (new SimpleDateFormat("yyyyMMddHHmmssz")).format(new Date(System.currentTimeMillis()));
			tableOps.flush(tableName, null, null, false);
			tableOps.compact(tableName, null, null, true, false);
		}
		catch (Exception e) {
			if (fireAndForget) {
				e.printStackTrace();
			}
			else {
				throw new IOException(e);
			}
		}
	}
}
