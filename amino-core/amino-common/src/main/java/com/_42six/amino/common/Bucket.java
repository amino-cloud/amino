package com._42six.amino.common;

import com._42six.amino.common.index.BitmapIndex;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Class to represent a bucket of data for a particular data source.
 * Each bucket class contains the data source from which it came from, the name of the bucket and
 * a MapWritable which contains the value of the bucket or the key value pairs represent. 
 *
 */
public final class Bucket implements WritableComparable<Bucket> {
	
	private Text bucketName;
	private Text bucketDataSource;
	private Text bucketValue;
	private Text bucketDisplayName;
	private Text bucketVisibility;
	private Text bucketHRVisibility;
	
	private Text domainName;
	private Text domainDescription;
	private long timestamp;

	private int hash;
	private final static String SEPARATOR = ".";
	
	public Bucket() throws IOException {
	}
	
	//public Bucket(final Text bucketDataSource, final Text bucketName, final Text bucketValue, final Text visibility) throws IOException {
   //             this(bucketDataSource, bucketName, bucketValue, new Text(""), visibility);
	//}
	
	public Bucket(final Text bucketDataSource, final Text bucketName, final Text bucketValue, final Text bucketDisplayName, final Text visibility, final Text humanReadableVisibility) throws IOException {
		this.bucketName = bucketName;
		this.bucketDataSource = bucketDataSource;
		this.bucketValue = bucketValue;
		this.bucketDisplayName = bucketDisplayName;
		this.bucketVisibility = visibility;
		this.bucketHRVisibility = humanReadableVisibility;
		domainName = new Text();
		domainDescription = new Text();
		this.computeHash();
	}
	
	//public Bucket(String source, String name, String value, String visibility) throws IOException{
	//  this(new Text(source.getBytes()), new Text(name.getBytes()), new Text(value.getBytes()), new Text(visibility.getBytes()));
	//}
	
	public Bucket(String source, String name, String value, String displayName, String visibility, String humanReadableVisibility) throws IOException{
		  this(new Text(source.getBytes()), new Text(name.getBytes()), new Text(value.getBytes()), new Text(displayName.getBytes()), new Text(visibility.getBytes()), new Text(humanReadableVisibility.getBytes()));
		}
	
	public Bucket(Bucket other) throws IOException{
	  this.bucketName = new Text(other.bucketName);
	  this.bucketDataSource = new Text(other.bucketDataSource);
	  this.bucketValue = new Text(other.bucketValue);
	  this.bucketDisplayName = new Text(other.bucketDisplayName);
	  this.bucketVisibility = new Text(other.bucketVisibility);
	  this.bucketHRVisibility = new Text(other.bucketHRVisibility);
	  this.domainName = new Text(other.domainName);
	  this.domainDescription = new Text(other.domainDescription);
	  this.computeHash();
	}
	
	/**
	 * Deserialize bucket object from DataInput
	 * @param dataIn a data input object to read from
	 */
	@Override
	public void readFields(DataInput dataIn) throws IOException {
		this.bucketDataSource = new Text(Text.readString(dataIn));
		this.bucketName = new Text(Text.readString(dataIn));
		this.bucketValue = new Text(Text.readString(dataIn));
		this.bucketDisplayName = new Text(Text.readString(dataIn));
		this.bucketVisibility = new Text(Text.readString(dataIn));
		this.bucketHRVisibility = new Text(Text.readString(dataIn));
		this.domainName = new Text(Text.readString(dataIn));
		this.domainDescription = new Text(Text.readString(dataIn));
		this.computeHash();
	}
	
	public void overrideBucketDataSourceWithDomain(Integer domain, String domainName, String domainDescription) throws IOException {
		if (domain != null) {
			this.bucketDataSource = new Text(domain.toString());
		}
		if (domainName != null) {
			this.domainName = new Text(domainName);
		}
		if (domainDescription != null) {
			this.domainDescription = new Text(domainDescription);
		}
		this.computeHash();
	}
	
	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public Text getDomainName()
	{
		return this.domainName;
	}
	
	public Text getDomainDescription()
	{
		return this.domainDescription;
	}
	
	/**
	 * Serialize bucket object to DataOutput
	 * @param dataOut data output to write to
	 */
	@Override
	public void write(DataOutput dataOut) throws IOException {
		this.bucketDataSource.write(dataOut);
		this.bucketName.write(dataOut);
		this.bucketValue.write(dataOut);
		this.bucketDisplayName.write(dataOut);
		this.bucketVisibility.write(dataOut);
		this.bucketHRVisibility.write(dataOut);
		this.domainName.write(dataOut);
		this.domainDescription.write(dataOut);
	}
	
	/**
	 * Compare one bucket to another bucket.
	 * @param otherBucket the bucket to compare against
	 * @return an integer negative if less, positive if greater, 0 if equal
	 */
	@Override
	public int compareTo(Bucket otherBucket) {
            if (this.bucketDataSource.compareTo(otherBucket.bucketDataSource) == 0) {
                if (this.bucketName.compareTo(otherBucket.bucketName) == 0) {
                    return this.bucketValue.compareTo(otherBucket.bucketValue);
                } else {
                    return this.bucketName.compareTo(otherBucket.bucketName);
                }
            } else {
                return this.bucketDataSource.compareTo(otherBucket.bucketDataSource);
            }
	}


	/**
	 * @return the bucketName
	 */
	public Text getBucketName() {
		return bucketName;
	}
	
	/**
	 * @return the bucketDisplayName
	 */
	public Text getBucketDisplayName() {
		return bucketDisplayName;
	}

	/**
	 * @return the bucketValue
	 */
	public Text getBucketValue() {
		return bucketValue;
	}
	
	public void setBucketValue(Text bucketValue) {
		this.bucketValue = bucketValue;
	}

	/**
	 * @return the bucketDataSource
	 */
	public Text getBucketDataSource() {
		return bucketDataSource;
	}
	
	public Text getBucketVisibility() {
		return bucketVisibility;
	}

	public void setBucketVisibility(Text bucketVisibility) {
		this.bucketVisibility = bucketVisibility;
	}
        
	public Text getBucketHRVisibility() {
		return bucketHRVisibility;
	}

	public void setBucketHRVisibility(Text bucketHRVisibility) {
		this.bucketHRVisibility = bucketHRVisibility;
	}

	@Override
	public String toString() {
		return this.bucketDataSource.toString() + SEPARATOR + this.bucketName.toString() + SEPARATOR + this.bucketValue;
	}
	
	/**
	 * This method computes the hash for the bucket using the assigned algorithm, and stores the value in the internal ByteBuffer.
	 * @throws IOException if the value can not be serialized, or if the hashing algorithm can not be found
	 */
	public void computeHash() throws IOException {
            this.hash = BitmapIndex.getBucketValueIndex(this);
	}
	
	/**
	 * This method will be used by the HashParitioner to figure out which reducer partition a given key should go to which reducer
	 */
	@Override
	public int hashCode() {
		return this.hash;
	}

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Bucket other = (Bucket) obj;
        if (this.bucketName != other.bucketName && (this.bucketName == null || !this.bucketName.equals(other.bucketName))) {
            return false;
        }
        if (this.bucketDataSource != other.bucketDataSource && (this.bucketDataSource == null || !this.bucketDataSource.equals(other.bucketDataSource))) {
            return false;
        }
        if (this.bucketValue != other.bucketValue && (this.bucketValue == null || !this.bucketValue.equals(other.bucketValue))) {
            return false;
        }
        return true;
    }
}
