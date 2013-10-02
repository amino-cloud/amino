package com._42six.amino.common.bigtable;

import com._42six.amino.common.bigtable.MutationProto.ColumnValue;
import com._42six.amino.common.bigtable.MutationProto.MutationCodec;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class Mutation implements Writable
{
	private byte [] row;	
	private List<ColumnValue> columnValues;		

	public Mutation()
	{
	}

	public Mutation(byte [] row)
	{
		this.row = row;
		columnValues = Lists.newArrayList();
	}

	public void addColumnValue(byte [] cf, byte [] cq, byte [] vis, long timestamp, boolean isDelete, byte [] value)
	{
		if(timestamp < 0)
		{
			timestamp = 0;
		}
		
		if(isDelete)
		{
			value = new byte[0];
		}

		ColumnValue cv = ColumnValue.newBuilder()
									.setColumnFamily(ByteString.copyFrom(cf))
								    .setColumnQualifier(ByteString.copyFrom(cq))
									.setColumnVisibility(ByteString.copyFrom(vis))
									.setTimestamp(timestamp)
									.setIsDelete(isDelete)
									.setValue(ByteString.copyFrom(value))
									.build();
		columnValues.add(cv);
	} 
	
	public byte [] getRow()
	{ 
		return row;
	}
	
	public List<ColumnValue> getColumnValues()
	{
		return columnValues;
	}

	@Override
	public void write(DataOutput output) throws IOException
	{
		MutationCodec mutationCodec = MutationCodec.newBuilder()
												   .setRow(ByteString.copyFrom(row))
												   .addAllColumnValues(columnValues)
												   .build();

		byte [] encodedMutation = mutationCodec.toByteArray();
		output.writeInt(encodedMutation.length);
		output.write(encodedMutation);
	}

	@Override
	public void readFields(DataInput input) throws IOException
	{
		int encodedMutationLength = input.readInt();
		byte [] encodedMutation = new byte[encodedMutationLength];
		
		input.readFully(encodedMutation);
		
		MutationCodec mutationCodec = MutationCodec.parseFrom(encodedMutation);	
		this.row = mutationCodec.getRow().toByteArray();	
		this.columnValues = mutationCodec.getColumnValuesList();
		
	}
}
