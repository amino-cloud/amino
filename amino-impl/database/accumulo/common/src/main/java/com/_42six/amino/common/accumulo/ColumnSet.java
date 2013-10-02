package com._42six.amino.common.accumulo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import org.apache.accumulo.core.data.Column;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class ColumnSet {
	private HashSet<Column> columns;	
	
	public ColumnSet() {
		this.columns = Sets.newHashSet();
	}

	public void add(Column c) {
		columns.add(Preconditions.checkNotNull(c));		
	}
	
	public boolean contains(Column c) {
		return columns.contains(Preconditions.checkNotNull(c));				
	}
	
	public String toBase64String() throws IOException {
		ByteArrayOutputStream byteArrayColumnSet = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(byteArrayColumnSet);
		for(Column c : columns) {
			c.write(dataOut);
		}
		return BaseEncoding.base64().encode(byteArrayColumnSet.toByteArray());
	}
	
	public static ColumnSet fromBase64String(String base64String) throws IOException {
		byte [] rawBytes = BaseEncoding.base64().decode(base64String);		
		ByteArrayInputStream bais = new ByteArrayInputStream(rawBytes);	
		DataInputStream dis = new DataInputStream(bais);
		// We can keep looping until we find the end of the file
		ColumnSet retVal = new ColumnSet();
		while(dis.available() > 0) {
			Column c = new Column();
			c.readFields(dis);
			retVal.add(c);
		}
		return retVal;
	}
	
	public Set<Column> getColumns()
	{
		return columns;
	}
}
