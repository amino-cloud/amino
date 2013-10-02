package com._42six.amino.common;

import com._42six.amino.common.index.BitmapIndex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BucketStripped implements WritableComparable<BucketStripped> {

	private int hashcode;
	private IntWritable cacheHash;
	private Text bucketValue;

	public BucketStripped() {
		cacheHash = new IntWritable();
		bucketValue = new Text();
	}
	
	public BucketStripped(int hashcode, IntWritable cacheHash, Text bucketValue) {
		this.hashcode = hashcode;
		this.cacheHash = cacheHash;
		this.bucketValue = bucketValue;
	}

    public BucketStripped(BucketStripped that){
        this.hashcode = that.hashcode;
        this.cacheHash = that.cacheHash;
        this.bucketValue = that.bucketValue;
    }

	/**
	 * Deserialize bucket object from DataInput
	 * @param dataIn a data input object to read from
	 */
	@Override
	public void readFields(DataInput dataIn) throws IOException {
		this.cacheHash.readFields(dataIn);
		this.bucketValue.readFields(dataIn);
		this.computeHash();
	}
	
	/**
	 * Serialize bucket object to DataOutput
	 * @param dataOut data output to write to
	 */
	@Override
	public void write(DataOutput dataOut) throws IOException {
		this.cacheHash.write(dataOut);
		this.bucketValue.write(dataOut);
	}
	
	/**
	 * Compare one bucket to another bucket.
	 * @param otherBucket the bucket to compare against
	 * @return an integer negative if less, positive if greater, 0 if equal
	 */
	@Override
	public int compareTo(BucketStripped otherBucket) {
		int val = this.bucketValue.compareTo(otherBucket.bucketValue);
		if (val == 0) {
			if (this.hashcode == otherBucket.hashcode) {
				return 0;
			}
			else {
				//val = (new Integer(this.hashcode)).compareTo(otherBucket.hashcode);
				return this.hashcode > otherBucket.hashcode ? 1 : -1;
			}
		}
		else {
			return val;
		}
	}
	
	/**
	 * This method computes the hash for the bucket using the assigned algorithm, and stores the value in the internal ByteBuffer.
	 * @throws IOException if the value can not be serialized, or if the hashing algorithm can not be found
	 */
	private void computeHash() throws IOException {
            this.hashcode = BitmapIndex.getBucketValueIndex(this);
	}
	
	/**
	 * This method will be used by the HashParitioner to figure out which reducer partition a given key should go to which reducer
	 */
	@Override
	public int hashCode() {
		return this.hashcode;
	}
	
	public static BucketStripped fromBucket(Bucket bucket) {
		return new BucketStripped(bucket.hashCode(), new IntWritable(BitmapIndex.getBucketCacheIndex(bucket)), new Text(bucket.getBucketValue()));
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BucketStripped other = (BucketStripped) obj;
		if (bucketValue == null) {
			if (other.bucketValue != null)
				return false;
		} else if (!bucketValue.equals(other.bucketValue))
			return false;
		if (cacheHash == null) {
			if (other.cacheHash != null)
				return false;
		} else if (!cacheHash.equals(other.cacheHash))
			return false;
		return true;
	}

	/**
	 * @return the cacheHash
	 */
	public IntWritable getCacheHash() {
		return cacheHash;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "BucketStripped [hashcode=" + hashcode + ", cacheHash="
				+ cacheHash + ", bucketValue=" + bucketValue + "]";
	}

	/**
	 * @param cacheHash the cacheHash to set
	 */
	public void setCacheHash(IntWritable cacheHash) {
		this.cacheHash = cacheHash;
	}

	/**
	 * @return the bucketValue
	 */
	public Text getBucketValue() {
		return bucketValue;
	}

	/**
	 * @param bucketValue the bucketValue to set
	 */
	public void setBucketValue(Text bucketValue) {
		this.bucketValue = bucketValue;
	}

}
