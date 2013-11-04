package com._42six.amino.bitmap;

import com._42six.amino.common.BucketStripped;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BucketKey implements WritableComparable {

    private BucketStripped bucketStripped;
    private int salt;


    public BucketStripped getBucketStripped() {
        return bucketStripped;
    }

    public void setBucketStripped(BucketStripped bucketStripped) {
        this.bucketStripped = bucketStripped;
    }

    public int getSalt(){
        return salt;
    }

    public void setSalt(int salt){
        this.salt = salt;
    }

    public BucketKey() {
        // EMPTY
    }

    public BucketKey(BucketStripped bucketStripped) {
        this.bucketStripped = bucketStripped;
    }

    public BucketKey(BucketStripped bucketStripped, int salt) {
        this(bucketStripped);
        this.salt = salt;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) { return false; }
        if (o == this) { return true; }
        if (o.getClass() != getClass()) { return false; }

        BucketKey other = (BucketKey) o;
        return new EqualsBuilder()
                .append(bucketStripped, other.bucketStripped)
                .append(salt, other.salt)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(bucketStripped)
                .append(salt)
                .toHashCode();
    }

    public int compareTo(Object o) {
        BucketKey other = (BucketKey) o;
        return new CompareToBuilder()
                .append(bucketStripped, other.bucketStripped)
                .append(salt, other.salt)
                .toComparison();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("bucketStripped", bucketStripped.toString())
                .append("salt", salt)
                .toString();
    }

    public void write(DataOutput dataOutput) throws IOException {
        bucketStripped.write(dataOutput);
        dataOutput.writeInt(salt);
    }

    public void readFields(DataInput dataInput) throws IOException {
        bucketStripped = new BucketStripped();
        bucketStripped.readFields(dataInput);
        salt = dataInput.readInt();
    }
}
