package com._42six.amino.bitmap.reverse;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReverseBitmapValue implements Writable
{
	protected int bucketValueIndex;
	protected String bucketName;
	protected String bucketValue;
	
	public ReverseBitmapValue()
	{
		
	}
	
	public ReverseBitmapValue(int bucketValueIndex, String bucketName, String bucketValue)
	{
		this.bucketValueIndex = bucketValueIndex;
		this.bucketName = bucketName;
		this.bucketValue = bucketValue;
	}

	@Override
	public void readFields(DataInput input) throws IOException 
	{
		bucketValueIndex = input.readInt();
		bucketName = input.readUTF();
		bucketValue = input.readUTF();
	}

	@Override
	public void write(DataOutput output) throws IOException 
	{
		output.writeInt(bucketValueIndex);
		output.writeUTF(bucketName);
		output.writeUTF(bucketValue);
	}

	public int getBucketValueIndex() {
		return bucketValueIndex;
	}

	public void setBucketValueIndex(int bucketValueIndex) {
		this.bucketValueIndex = bucketValueIndex;
	}

	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	public String getBucketValue() {
		return bucketValue;
	}

	public void setBucketValue(String bucketValue) {
		this.bucketValue = bucketValue;
	}

    public String toString(){
        return "bvIndex: " + bucketValueIndex + " bucketName: " + bucketName + " bucketValue: " + bucketValue;
    }

}
