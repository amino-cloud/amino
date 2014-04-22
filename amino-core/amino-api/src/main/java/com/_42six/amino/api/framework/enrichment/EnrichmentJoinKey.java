package com._42six.amino.api.framework.enrichment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com._42six.amino.common.BucketStripped;

@SuppressWarnings("rawtypes")
public class EnrichmentJoinKey implements WritableComparable<EnrichmentJoinKey>
{
	private String naturalKey;
	private int type;
	private BucketStripped bucket;
	
	public static final int TYPE_SUBJECT = 1;
	public static final int TYPE_ENRICHMENT = 0;
	
	public EnrichmentJoinKey()
	{
		
	}
	
	public EnrichmentJoinKey(String naturalKey, int type)
	{
		this.naturalKey = naturalKey;
		this.type = type;
	}

	@Override
	public void readFields(DataInput input) throws IOException 
	{
		naturalKey = input.readUTF();
		type = input.readInt();
		if (type == TYPE_SUBJECT)
		{
			bucket = new BucketStripped();
			bucket.readFields(input);
		}
	}

	@Override
	public void write(DataOutput output) throws IOException 
	{
		output.writeUTF(naturalKey);
		output.writeInt(type);
		if (type == TYPE_SUBJECT) bucket.write(output);
	}

	public String getNaturalKey() {
		return naturalKey;
	}

	public void setNaturalKey(String naturalKey) {
		this.naturalKey = naturalKey;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}
	
	public BucketStripped getBucket() {
		return bucket;
	}

	public void setBucket(BucketStripped bucket) {
		this.bucket = bucket;
	}

	@Override
	public int compareTo(EnrichmentJoinKey other) 
	{
		return this.naturalKey.compareTo(other.naturalKey);
	}

}
