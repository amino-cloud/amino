package com._42six.amino.bitmap.reverse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ReverseHypothesisValue implements Writable
{

	private int indexPos;
	private String datasource;
	private String bucketName;
	private int salt;
	private String bucketValue;
	private String visibility;
	
	public ReverseHypothesisValue()
	{
		
	}
	
	public ReverseHypothesisValue(int indexPos, String datasource, String bucketName, int salt, String bucketValue, String visibility)
	{
		this.indexPos = indexPos;
		this.datasource = datasource;
		this.bucketName = bucketName;
		this.salt = salt;
		this.bucketValue = bucketValue;
		this.visibility = visibility;
	}

	@Override
	public void readFields(DataInput input) throws IOException 
	{
		indexPos = input.readInt();
		datasource = input.readUTF();
		bucketName = input.readUTF();
		salt = input.readInt();
		bucketValue = input.readUTF();
		visibility = input.readUTF();
	}

	@Override
	public void write(DataOutput output) throws IOException 
	{
		output.writeInt(indexPos);
		output.writeUTF(datasource);
		output.writeUTF(bucketName);
		output.writeInt(salt);
		output.writeUTF(bucketValue);
		output.writeUTF(visibility);
	}

	public int getIndexPos() {
		return indexPos;
	}

	public void setIndexPos(int indexPos) {
		this.indexPos = indexPos;
	}

	public String getDatasource() {
		return datasource;
	}

	public void setDatasource(String datasource) {
		this.datasource = datasource;
	}

	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
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

	public String getVisibility() {
		return visibility;
	}

	public void setVisibility(String visibility) {
		this.visibility = visibility;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((bucketName == null) ? 0 : bucketName.hashCode());
		result = prime * result
				+ ((bucketValue == null) ? 0 : bucketValue.hashCode());
		result = prime * result
				+ ((datasource == null) ? 0 : datasource.hashCode());
		result = prime * result + indexPos;
		result = prime * result + salt;
		result = prime * result
				+ ((visibility == null) ? 0 : visibility.hashCode());
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
		if (bucketName == null) {
			if (other.bucketName != null)
				return false;
		} else if (!bucketName.equals(other.bucketName))
			return false;
		if (bucketValue == null) {
			if (other.bucketValue != null)
				return false;
		} else if (!bucketValue.equals(other.bucketValue))
			return false;
		if (datasource == null) {
			if (other.datasource != null)
				return false;
		} else if (!datasource.equals(other.datasource))
			return false;
		if (indexPos != other.indexPos)
			return false;
		if (salt != other.salt)
			return false;
		if (visibility == null) {
			if (other.visibility != null)
				return false;
		} else if (!visibility.equals(other.visibility))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ReverseHypothesisValue [indexPos=" + indexPos + ", datasource="
				+ datasource + ", bucketName=" + bucketName + ", salt=" + salt
				+ ", bucketValue=" + bucketValue + ", visibility=" + visibility
				+ "]";
	}

}
