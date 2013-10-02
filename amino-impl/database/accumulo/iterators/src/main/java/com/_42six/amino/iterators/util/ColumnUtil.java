package com._42six.amino.iterators.util;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;

public class ColumnUtil 
{

	/**
	 * This will do comparison based on column family and column qualifier
	 *
	 * key the key that is being compared
     * column the column that we are comparing against 
     * @return a boolean based on whether a column is in a key
	 */
	public static boolean keyHasColumn(Key key, Column column) 
	{
		ByteSequence keyCf = key.getColumnFamilyData();		
		ByteSequence colCf = new ArrayByteSequence(column.getColumnFamily());
		if(keyCf.equals(colCf) == false) 
		{
			return false;
		}

		ByteSequence keyCq = key.getColumnQualifierData();
		ByteSequence colCq = new ArrayByteSequence(column.getColumnQualifier());
	
		return keyCq.equals(colCq);	
	}		
	

}
