package com._42six.amino.common.bigtable;

import com._42six.amino.common.bigtable.MutationProto.ColumnValue;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class MutationTest
{
	@Test @Ignore
	public void testWritable() throws Exception
	{
		final String row = "Hello";

		Mutation mutation = new Mutation(row.getBytes());
		mutation.addColumnValue("world".getBytes(), "name".getBytes(), new byte[0], 0L, false, "cookie".getBytes());
		mutation.addColumnValue("two".getBytes(), "three".getBytes(), "four".getBytes(), 1000, true, "shouldnotbethere".getBytes());
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);	
		
		mutation.write(dos);
		
		byte [] output = bos.toByteArray();	
		
		ByteArrayInputStream bis = new ByteArrayInputStream(output);	
		DataInputStream dis = new DataInputStream(bis);

		Mutation m = new Mutation();
		m.readFields(dis);
		
		System.out.println("Row: " + new String(m.getRow()));
		for(ColumnValue cv : m.getColumnValues())
		{
			printColumnValue(cv);			
		}	
				
	}
	
	private static void printColumnValue(ColumnValue cv)
	{
		System.out.println("Column Family: " + new String(cv.getColumnFamily().toByteArray()));		
		System.out.println("Column Qualifier: " + new String(cv.getColumnQualifier().toByteArray()));		
		System.out.println("Column Visibility: " + new String(cv.getColumnVisibility().toByteArray()));
		System.out.println("Timestamp: " + cv.getTimestamp());
		System.out.println("IsDelete: " + cv.getIsDelete());
		System.out.println("Value: " + new String(cv.getValue().toByteArray()));
	}
	
}
