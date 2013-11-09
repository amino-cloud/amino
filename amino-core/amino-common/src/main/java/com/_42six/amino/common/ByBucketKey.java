package com._42six.amino.common;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ByBucketKey implements WritableComparable<ByBucketKey>{

    private Text bucketValue;
    private int binNumber = -1;
    private IntWritable bucketNameIndex;
    private IntWritable datasourceNameIndex;
    private IntWritable visibilityIndex;
    private int salt = -1;

    public Text getBucketValue() {
        return bucketValue;
    }

    public void setBucketValue(Text bucketValue) {
        this.bucketValue = bucketValue;
    }

    public int getBinNumber() {
        return binNumber;
    }

    public void setBinNumber(int binNumber) {
        this.binNumber = binNumber;
    }

    public IntWritable getBucketNameIndex() {
        return bucketNameIndex;
    }

    public void setBucketNameIndex(IntWritable bucketNameIndex) {
        this.bucketNameIndex = bucketNameIndex;
    }

    public IntWritable getDatasourceNameIndex() {
        return datasourceNameIndex;
    }

    public void setDatasourceNameIndex(IntWritable datasourceNameIndex) {
        this.datasourceNameIndex = datasourceNameIndex;
    }

    public int getSalt() {
        return salt;
    }

    public void setSalt(int salt) {
        this.salt = salt;
    }

    public IntWritable getVisibilityIndex() {
        return visibilityIndex;
    }

    public void setVisibilityIndex(IntWritable visibilityIndex) {
        this.visibilityIndex = visibilityIndex;
    }

    public ByBucketKey(){
        // EMPTY
    }

    public ByBucketKey(Text bucketValue, int binNumber, IntWritable bucketNameIndex, IntWritable datasourceNameIndex, IntWritable visibilityIndex){
        this.bucketValue = bucketValue;
        this.binNumber = binNumber;
        this.bucketNameIndex = bucketNameIndex;
        this.datasourceNameIndex = datasourceNameIndex;
        this.visibilityIndex = visibilityIndex;
    }

    public ByBucketKey(Text bucketValue, int binNumber, IntWritable bucketNameIndex, IntWritable datasourceNameIndex, IntWritable visibilityIndex, int salt){
        this(bucketValue,binNumber, bucketNameIndex, datasourceNameIndex, visibilityIndex);
        this.salt = salt;
    }

    /**
     * Deserialize ByBucketKey object from DataInput
     * @param dataIn a data input object to read from
     */
    @Override
    public void readFields(DataInput dataIn) throws IOException {
        if(this.bucketValue == null) { this.bucketValue = new Text(); }
        if(this.bucketNameIndex == null) { this.bucketNameIndex = new IntWritable(); }
        if(this.datasourceNameIndex == null) { this.datasourceNameIndex = new IntWritable(); }
        if(this.visibilityIndex == null) { this.visibilityIndex = new IntWritable(); }

        this.bucketValue.readFields(dataIn);
        this.bucketNameIndex.readFields(dataIn);
        this.datasourceNameIndex.readFields(dataIn);
        this.binNumber = dataIn.readInt();
        this.visibilityIndex.readFields(dataIn);
        this.salt = dataIn.readByte();
    }

    /**
     * Serialize ByBucketKey object to DataOutput
     * @param dataOut data output to write to
     */
    @Override
    public void write(DataOutput dataOut) throws IOException {
        this.bucketValue.write(dataOut);
        this.bucketNameIndex.write(dataOut);
        this.datasourceNameIndex.write(dataOut);
        dataOut.writeInt(this.binNumber);
        this.visibilityIndex.write(dataOut);
        dataOut.writeByte(this.salt);
    }

    /**
     * Compare one ByBucketKey to another ByBucketKey.
     * @param other the ByBucketKey to compare against
     * @return an integer negative if less, positive if greater, 0 if equal
     */
    @Override
    public int compareTo(ByBucketKey other) {
        int comparison;

        // Lexicographically sort.  Must append : otherwise 1:12345:foo will come after 10:12345:foo
        if(this.binNumber != other.binNumber){
            return (String.valueOf(this.binNumber) + ':').compareTo(String.valueOf(other.binNumber) + ':');
        }

        if(this.datasourceNameIndex == null){
            return -1;
        } else {
            if(other.datasourceNameIndex == null){
                return 1;
            } else {
                comparison = this.datasourceNameIndex.compareTo(other.datasourceNameIndex);
                if(comparison != 0){ return comparison; }
            }
        }

        if(this.bucketNameIndex == null){
            return -1;
        } else {
            if(other.bucketNameIndex == null){
                return 1;
            } else {
                comparison = this.bucketNameIndex.compareTo(other.bucketNameIndex);
                if(comparison != 0){ return comparison; }
            }
        }

        if(this.bucketValue == null){
            return -1;
        } else {
            if(other.bucketValue == null){
                return 1;
            } else {
                comparison = this.bucketValue.compareTo(other.bucketValue);
                if(comparison != 0){ return comparison; }
            }
        }

        // TODO Technically this should be lexicographically sorted, but since we shouldn't have more than 9 salts, this
        // way is faster.
        if(this.salt != other.salt){
            return this.salt < other.salt ? -1 : 1;
        }

        if(this.visibilityIndex == null){
            return -1;
        } else {
            if(other.visibilityIndex == null){
                return 1;
            } else {
                comparison = this.visibilityIndex.compareTo(other.visibilityIndex);
                if(comparison != 0){ return comparison; }
            }
        }

        return 0;
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

        final ByBucketKey other = (ByBucketKey) obj;

        // binNumber
        if(this.binNumber == -1){
            if(other.binNumber != -1){
                return false;
            }
        }

        // bucketNameIndex
        if (bucketNameIndex == null) {
            if (other.bucketNameIndex != null)
                return false;
        } else if (!bucketNameIndex.equals(other.bucketNameIndex))
            return false;

        // datasourceNameIndex
        if (datasourceNameIndex == null) {
            if (other.datasourceNameIndex != null)
                return false;
        } else if (!datasourceNameIndex.equals(other.datasourceNameIndex))
            return false;

        // bucketValue
        if (bucketValue == null) {
            if (other.bucketValue != null)
                return false;
        } else if (!bucketValue.equals(other.bucketValue))
            return false;

        // salt
        if (salt == -1) {
            if (other.salt != -1)
                return false;
        } else if (salt != other.salt)
            return false;

        // visibilityIndex
        if (visibilityIndex == null) {
            if (other.visibilityIndex != null)
                return false;
        } else if (!visibilityIndex.equals(other.visibilityIndex))
            return false;

        return true;
    }


    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("binNumber", binNumber)
                .append("datasourceNameIndex", datasourceNameIndex)
                .append("bucketNameIndex", bucketNameIndex)
                .append("salt", salt)
                .append("bucketValue", bucketValue)
                .append("visibilityIndex", visibilityIndex).toString();
    }
}
