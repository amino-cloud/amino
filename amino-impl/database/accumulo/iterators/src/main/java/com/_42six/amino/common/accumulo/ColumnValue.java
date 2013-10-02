package com._42six.amino.common.accumulo;

import com.google.common.io.BaseEncoding;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Value;

import java.io.*;


public class ColumnValue 
{
	private Column column;
	private Value value;

	public ColumnValue(byte [] cq, byte [] cf, byte [] vis, Value value) 
	{
		this.column = new Column(cq, cf, vis);
		this.value = value;	
	}

	public ColumnValue(Column col, Value value) 
	{
		this.column = col;
		this.value = value;
	}
	
	public String toBase64String() throws IOException 
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		column.write(dos);
		value.write(dos);
		return BaseEncoding.base64().encode(baos.toByteArray());
	}

	public static ColumnValue fromBase64String(final String base64String) throws IOException
	{
		byte [] rawBytes = BaseEncoding.base64().decode(base64String);
		ByteArrayInputStream bais = new ByteArrayInputStream(rawBytes);
		DataInputStream dis = new DataInputStream(bais);
		Column c = new Column();
		c.readFields(dis);
		Value v = new Value();
		v.readFields(dis);
		return new ColumnValue(c, v);
	}		
	
	public Column getColumn()
	{
		return column;
	}
	
	public Value getValue() 
	{
		return value;
	}
}
