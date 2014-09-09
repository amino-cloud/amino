package com._42six.amino.common;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ByBucketKey implements WritableComparable<ByBucketKey>{

    private Text bucketValue;
    private int binNumber = -1;
    private Text bucketName;
    private VIntWritable datasourceNameIndex;
    private Text visibility;
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

    public Text getBucketName() {
        return bucketName;
    }

    public void setBucketName(Text bucketName) {
        this.bucketName = bucketName;
    }

    public VIntWritable getDatasourceNameIndex() {
        return datasourceNameIndex;
    }

    public void setDatasourceNameIndex(VIntWritable datasourceNameIndex) {
        this.datasourceNameIndex = datasourceNameIndex;
    }

    public int getSalt() {
        return salt;
    }

    public void setSalt(int salt) {
        this.salt = salt;
    }

    public Text getVisibility() {
        return visibility;
    }

    public void setVisibility(Text visibility) {
        this.visibility = visibility;
    }

    public ByBucketKey(){
        // EMPTY
    }

    public ByBucketKey(Text bucketValue, int binNumber, Text bucketName, VIntWritable datasourceNameIndex,
                       Text visibility){
        this.bucketValue = bucketValue;
        this.binNumber = binNumber;
        this.bucketName = bucketName;
        this.datasourceNameIndex = datasourceNameIndex;
        this.visibility = visibility;
    }

    public ByBucketKey(Text bucketValue, int binNumber, Text bucketName, int datasourceNameIndex, Text visibility){
        this(bucketValue, binNumber, bucketName, new VIntWritable(datasourceNameIndex), visibility);
    }

    public ByBucketKey(Text bucketValue, int binNumber, Text bucketName, VIntWritable datasourceNameIndex,
                       Text visibility, int salt){
        this(bucketValue,binNumber, bucketName, datasourceNameIndex, visibility);
        this.salt = salt;
    }

    /**
     * Deserialize ByBucketKey object from DataInput
     * @param dataIn a data input object to read from
     */
    @Override
    public void readFields(DataInput dataIn) throws IOException {
        if(this.bucketValue == null) { this.bucketValue = new Text(); }
        if(this.bucketName == null) { this.bucketName = new Text(); }
        if(this.datasourceNameIndex == null) { this.datasourceNameIndex = new VIntWritable(); }
        if(this.visibility == null) { this.visibility = new Text(); }

        this.bucketValue.readFields(dataIn);
        this.bucketName.readFields(dataIn);
        this.datasourceNameIndex.readFields(dataIn);
        this.binNumber = dataIn.readInt();
        this.visibility.readFields(dataIn);
        this.salt = dataIn.readByte();
    }

    /**
     * Serialize ByBucketKey object to DataOutput
     * @param dataOut data output to write to
     */
    @Override
    public void write(DataOutput dataOut) throws IOException {
        this.bucketValue.write(dataOut);
        this.bucketName.write(dataOut);
        this.datasourceNameIndex.write(dataOut);
        dataOut.writeInt(this.binNumber);
        this.visibility.write(dataOut);
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

        if(this.bucketName == null){
            return -1;
        } else {
            if(other.bucketName == null){
                return 1;
            } else {
                comparison = this.bucketName.compareTo(other.bucketName);
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

        if(this.visibility == null){
            return -1;
        } else {
            if(other.visibility == null){
                return 1;
            } else {
                comparison = this.visibility.compareTo(other.visibility);
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

        // bucketName
        if (bucketName == null) {
            if (other.bucketName != null)
                return false;
        } else if (!bucketName.equals(other.bucketName))
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

        // visibility
        if (visibility == null) {
            if (other.visibility != null)
                return false;
        } else if (!visibility.equals(other.visibility))
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
                .append("bucketName", bucketName)
                .append("salt", salt)
                .append("bucketValue", bucketValue)
                .append("visibility", visibility).toString();
    }
}
