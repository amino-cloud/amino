package com._42six.amino.bitmap;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StatsKey implements WritableComparable
{
    public String row;
    public String val;
	public String bucketName;
    public int salt;
    public String vis;
	public int bitmapIndex;

	public StatsKey() {
        // EMPTY
    }

    public StatsKey(String row, String val, String bucketName, String vis) {
        this.row = row;
        this.val = val;
        this.bucketName = bucketName;
        this.salt = 0;
        this.vis = vis;
        this.bitmapIndex = 0;
    }

    public StatsKey(String row, String val, String bucketName, String vis, int salt, int bitmapIndex){
        this.row = row;
        this.val = val;
        this.bucketName = bucketName;
        this.salt = salt;
        this.vis = vis;
        this.bitmapIndex = bitmapIndex;
    }

    public String getRow() {
        return row;
    }

    public String getVal() {
        return val;
    }

    public int getSalt() {
        return salt;
    }

    public void setSalt(int salt){
        this.salt = salt;
    }

    public String getVis() {
        return vis;
    }

    public String getBucketName()
    {
    	return bucketName;
    }

	@Override
    public boolean equals(Object o) {
        if (o == null) { return false; }
        if (o == this) { return true; }
        if (o.getClass() != getClass()) { return false; }

        StatsKey other = (StatsKey) o;
        return new EqualsBuilder()
                .append(row, other.row)
                .append(val, other.val)
                .append(bucketName, other.bucketName)
                .append(salt, other.salt)
                .append(bitmapIndex, other.bitmapIndex)
                .append(vis, other.vis)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(row)
                .append(val)
                .append(bucketName)
                .append(salt)
                .append(bitmapIndex)
                .append(vis)
                .toHashCode();
    }

    public int compareTo(Object o) {
        StatsKey other = (StatsKey) o;
        return new CompareToBuilder()
                .append(row, other.row)
                .append(val, other.val)
                .append(bucketName, other.bucketName)
                .append(salt, other.salt)
                .append(bitmapIndex, other.bitmapIndex)
                .append(vis, other.vis)
                .toComparison();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("row", row)
                .append("val", val)
                .append("bucketName", bucketName)
                .append("salt", salt)
                .append("bitmapIndex", bitmapIndex)
                .append("vis", vis)
                .toString();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(row);
        dataOutput.writeUTF(val);
        dataOutput.writeUTF(bucketName);
        dataOutput.writeInt(salt);
        dataOutput.writeUTF(vis);
        dataOutput.writeInt(bitmapIndex);
    }

    public void readFields(DataInput dataInput) throws IOException {
        row = dataInput.readUTF();
        val = dataInput.readUTF();
        bucketName = dataInput.readUTF();
        salt = dataInput.readInt();
        vis = dataInput.readUTF();
        bitmapIndex = dataInput.readInt();
    }
}
