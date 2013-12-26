package com._42six.amino.bitmap.reverse;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReverseBitmapKey implements WritableComparable
{
	private int shard;
	private int salt;
	private int featureId;
	private String featureValue;
	private IntWritable visibility;
    private IntWritable datasource;
    private IntWritable bucketName;

    public ReverseBitmapKey()
	{
		// Empty
	}
	
	public ReverseBitmapKey(int shard, int salt, IntWritable datasource, IntWritable bucketName, int featureId, String featureValue, IntWritable visibility)
	{
		this.shard = shard;
		this.salt = salt;
        this.datasource = datasource;
        this.bucketName = bucketName;
		this.featureId = featureId;
		this.featureValue = featureValue;
		this.visibility = visibility;
	}

    @Override
    public String toString(){
        return new ToStringBuilder(this)
                .append("shard", shard)
                .append("salt", salt)
                .append("featureId", featureId)
                .append("featureValue", featureValue)
                .append("visibility", visibility)
                .append("datasource", datasource)
                .append("bucketName", bucketName)
                .toString();
    }

	@Override
	public void readFields(DataInput input) throws IOException 
	{
        if(datasource == null){ datasource = new IntWritable();}
        if(bucketName == null){ bucketName = new IntWritable();}
        if(visibility == null){ visibility = new IntWritable();}

		shard = input.readInt();
		salt = input.readInt();
        datasource.readFields(input);
        bucketName.readFields(input);
		featureId = input.readInt();
		featureValue = input.readUTF();
        visibility.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException 
	{
		output.writeInt(shard);
		output.writeInt(salt);
        datasource.write(output);
        bucketName.write(output);
		output.writeInt(featureId);
		output.writeUTF(featureValue);
        visibility.write(output);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + featureId;
		result = prime * result
				+ ((featureValue == null) ? 0 : featureValue.hashCode());
		result = prime * result + salt;
		result = prime * result + shard;
        result = prime * result +
                ((datasource == null) ? 0 : datasource.hashCode());
        result = prime * result +
                ((bucketName == null) ? 0 : bucketName.hashCode());
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
		ReverseBitmapKey other = (ReverseBitmapKey) obj;
		if (featureId != other.featureId)
			return false;
		if (featureValue == null) {
			if (other.featureValue != null)
				return false;
		} else if (!featureValue.equals(other.featureValue))
			return false;
        if(datasource == null || other.datasource == null || !datasource.equals(other.datasource)){
            return false;
        }
        if(bucketName == null || other.bucketName == null || !bucketName.equals(other.bucketName)){
            return false;
        }
        if(visibility == null || other.visibility == null || !visibility.equals(other.visibility)){
            return false;
        }

		if (salt != other.salt)
			return false;
		if (shard != other.shard)
			return false;
		return true;
	}
	
	public int compareTo(Object o) {
        final ReverseBitmapKey other = (ReverseBitmapKey) o;
        return new CompareToBuilder()
                .append(shard, other.shard)
                .append(salt, other.salt)
                .append(featureId, other.featureId)
                .append(featureValue, other.featureValue)
                .append(datasource, other.datasource)
                .append(bucketName, other.bucketName)
                .toComparison();
    }

	public int getShard() {
		return shard;
	}

	public void setShard(int shard) {
		this.shard = shard;
	}

	public int getSalt() {
		return salt;
	}

	public void setSalt(int salt) {
		this.salt = salt;
	}

	public int getFeatureId() {
		return featureId;
	}

	public void setFeatureId(int featureId) {
		this.featureId = featureId;
	}

	public String getFeatureValue() {
		return featureValue;
	}

	public void setFeatureValue(String featureValue) {
		this.featureValue = featureValue;
	}

	public IntWritable getVisibility() {
		return visibility;
	}

	public void setVisibility(IntWritable visibility) {
		this.visibility = visibility;
	}

    public IntWritable getDatasource() {
        return datasource;
    }

    public void setDatasource(IntWritable datasource) {
        this.datasource = datasource;
    }

    public IntWritable getBucketName() {
        return bucketName;
    }

    public void setBucketName(IntWritable bucketName) {
        this.bucketName = bucketName;
    }

}
