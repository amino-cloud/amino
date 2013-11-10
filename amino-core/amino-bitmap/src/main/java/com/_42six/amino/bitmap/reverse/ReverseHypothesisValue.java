package com._42six.amino.bitmap.reverse;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReverseHypothesisValue implements Writable
{

	private int indexPos;
    private IntWritable datasourceIdx;
    private IntWritable bucketNameIdx;
	private int salt;
    private String bucketValue;
    private IntWritable visibilityIdx;
	
	public ReverseHypothesisValue()
	{
		
	}
	
	public ReverseHypothesisValue(int indexPos, IntWritable datasourceIdx, IntWritable bucketNameIdx, int salt, String bucketValue, IntWritable visibilityIdx)
	{
		this.indexPos = indexPos;
		this.datasourceIdx = datasourceIdx;
		this.bucketNameIdx = bucketNameIdx;
		this.salt = salt;
		this.bucketValue = bucketValue;
		this.visibilityIdx = visibilityIdx;
	}

	@Override
	public void readFields(DataInput input) throws IOException 
	{
        if(datasourceIdx == null){
            datasourceIdx = new IntWritable();}
        if(bucketNameIdx == null){
            bucketNameIdx = new IntWritable();}
        if(visibilityIdx == null){
            visibilityIdx = new IntWritable();}

		indexPos = input.readInt();
		datasourceIdx.readFields(input);
		bucketNameIdx.readFields(input);
		salt = input.readInt();
		bucketValue = input.readUTF();
		visibilityIdx.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException 
	{
		output.writeInt(indexPos);
		datasourceIdx.write(output);
		bucketNameIdx.write(output);
		output.writeInt(salt);
		output.writeUTF(bucketValue);
		visibilityIdx.write(output);
	}

	public int getIndexPos() {
		return indexPos;
	}

	public void setIndexPos(int indexPos) {
		this.indexPos = indexPos;
	}

	public IntWritable getDatasourceIdx() {
		return datasourceIdx;
	}

	public void setDatasourceIdx(IntWritable datasourceIdx) {
		this.datasourceIdx = datasourceIdx;
	}

	public IntWritable getBucketNameIdx() {
		return bucketNameIdx;
	}

	public void setBucketNameIdx(IntWritable bucketNameIdx) {
		this.bucketNameIdx = bucketNameIdx;
	}

	public int getSalt() {
		return salt;
	}

	public void setSalt(int salt) {
		this.salt = salt;
	}

	public String getBucketValue() {
		return bucketValue;
	}

	public void setBucketValue(String bucketValue) {
		this.bucketValue = bucketValue;
	}

	public IntWritable getVisibilityIdx() {
		return visibilityIdx;
	}

	public void setVisibilityIdx(IntWritable visibilityIdx) {
		this.visibilityIdx = visibilityIdx;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((bucketNameIdx == null) ? 0 : bucketNameIdx.hashCode());
		result = prime * result
				+ ((bucketValue == null) ? 0 : bucketValue.hashCode());
		result = prime * result
				+ ((datasourceIdx == null) ? 0 : datasourceIdx.hashCode());
		result = prime * result + indexPos;
		result = prime * result + salt;
		result = prime * result
				+ ((visibilityIdx == null) ? 0 : visibilityIdx.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ReverseHypothesisValue other = (ReverseHypothesisValue) obj;
		if (bucketNameIdx == null) {
			if (other.bucketNameIdx != null)
				return false;
		} else if (!bucketNameIdx.equals(other.bucketNameIdx))
			return false;
		if (bucketValue == null) {
			if (other.bucketValue != null)
				return false;
		} else if (!bucketValue.equals(other.bucketValue))
			return false;
		if (datasourceIdx == null) {
			if (other.datasourceIdx != null)
				return false;
		} else if (!datasourceIdx.equals(other.datasourceIdx))
			return false;
		if (indexPos != other.indexPos)
			return false;
		if (salt != other.salt)
			return false;
		if (visibilityIdx == null) {
			if (other.visibilityIdx != null)
				return false;
		} else if (!visibilityIdx.equals(other.visibilityIdx))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ReverseHypothesisValue [indexPos=" + indexPos + ", datasourceIdx="
				+ datasourceIdx + ", bucketNameIdx=" + bucketNameIdx + ", salt=" + salt
				+ ", bucketValue=" + bucketValue + ", visibilityIdx=" + visibilityIdx
				+ "]";
	}

}
