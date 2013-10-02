package com._42six.amino.iterators;

import com._42six.amino.common.accumulo.ColumnSet;
import com._42six.amino.common.accumulo.ColumnValue;
import com._42six.amino.iterators.util.ColumnUtil;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

public class PartialRowIterator extends WrappingIterator {

	private static final Logger logger = Logger.getLogger(PartialRowIterator.class);	
	
	public static final String OPTION_COLUMNS_TO_KEEP = "columns_to_keep";
	public static final String OPTION_COLUMN_VALUE_TO_FILTER_BY = "column_value_to_filter_by";

	private Key topKey = null;
	private Value topValue = null;	
	
	private ColumnSet columnsToKeep;
	private ColumnValue rowFilter;
	
	public static SortedMap<Key, Value> decodeFromKeyValue(Key key, Value value) throws IOException
	{
		TreeMap<Key, Value> retVal = new TreeMap<Key, Value>();
		ByteArrayInputStream bais = new ByteArrayInputStream(value.get());
		DataInputStream dis = new DataInputStream(bais);
		while(dis.available() > 0) 
		{
			Key k = new Key();
			k.readFields(dis);
			Value v = new Value();
			v.readFields(dis);
			retVal.put(k, v);
		}
		return retVal;
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String , String> options, 
					IteratorEnvironment env) throws IOException 
	{
		setSource(source);
		init(options);
	}
	
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) 
	{
		throw new UnsupportedOperationException("Can not deep copy inside of a Partial Row Iterator");
	}

	@Override
	public Key getTopKey() 
	{
		return topKey;
	}
	
	@Override
	public Value getTopValue() 
	{
		return topValue;
	}

	@Override
	public boolean hasTop()
	{
		return topKey != null || super.hasTop();
	}
	
	@Override
	public void next() throws IOException 
	{
		topKey = null;
		topValue = null;
		getNextRow();
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException
	{
		topKey = null;
		topValue = null;
		super.seek(range, columnFamilies, inclusive);
		getNextRow();
	}
	
	private void init(Map<String,String> options) throws IOException 
	{
		if(!options.containsKey(OPTION_COLUMNS_TO_KEEP)) 
		{
			throw new RuntimeException(OPTION_COLUMNS_TO_KEEP + " must be set!");	
		}
		
		this.columnsToKeep = ColumnSet.fromBase64String(options.get(OPTION_COLUMNS_TO_KEEP));
		this.rowFilter = ColumnValue.fromBase64String(options.get(OPTION_COLUMN_VALUE_TO_FILTER_BY));
	}
	
	private void getNextRow() throws IOException
	{
		SortedKeyValueIterator<Key, Value> source = getSource();	
		if(!source.hasTop()) 
		{
			return;
		}
		
		Key top = source.getTopKey();
		Text curRow = top.getRow();
		boolean discardRow = true;
		ArrayList<KeyValue> toKeep = new ArrayList<KeyValue>();
		while(source.hasTop()) {
			top = source.getTopKey();

			if(!sameRow(top, curRow)) 
			{
				if(discardRow == true) 
				{
					toKeep.clear();	
					curRow = topKey.getRow();
				}
				else 
				{
					topKey = new Key(curRow);
					topValue = new Value(encodeKeyValueList(toKeep));
					return;
				}
			}
			
			if(rowFilter != null && ColumnUtil.keyHasColumn(top, rowFilter.getColumn()) 
			  && rowFilter.getValue().equals(source.getTopValue())) 
 		    {
				discardRow = false;
			} 
			
			if(columnsToKeep.contains(getColumnFromKey(top)))
			{
				toKeep.add(new KeyValue(top, source.getTopValue()));
			}

			source.next();
		}
	}	

	private boolean sameRow(Key testKey, Text expectedRow) 
	{
		return testKey.getRow().equals(expectedRow);
	}	
	
	private static Column getColumnFromKey(final Key k) 
	{
		byte [] cf = k.getColumnFamilyData().getBackingArray();
		byte [] cq = k.getColumnQualifierData().getBackingArray();
		byte [] cv = k.getColumnVisibilityData().getBackingArray();
		String vis = new String(cv);
		// TODO (soup) Look into turning this into a null when inserting columns
		if(vis.isEmpty()) 
		{
		  
			cv = null;
		}
		return new Column(cf, cq, cv);
	}			

	private static class KeyValue 
	{
		private final Key k;
		private final Value v;

		public KeyValue(Key k, Value v) 
		{
			this.k = k;
			this.v = v;
		}
		
		public Key getKey() 
		{
			return k;
		}
		
		public Value getValue() 
		{
			return v;
		}
	}		

	private byte [] encodeKeyValueList(ArrayList<KeyValue> keysAndValues) throws IOException 
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		for(KeyValue kv : keysAndValues) 
		{
			kv.getKey().write(dos);
			kv.getValue().write(dos);
		}
		return baos.toByteArray();
	}
}
