package com._42six.amino.bitmap;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BitmapKey implements WritableComparable {
	
	public enum KeyType {
        FEATURE,
        BUCKET
    }

    public KeyType type;
    public String row;
    public String val;
    public int salt;
    public String vis;

    public BitmapKey() {

    }

    public BitmapKey(KeyType type, String row, String val, int salt, String vis) {
        this.type = type;
        this.row = row;
        this.val = val;
        this.salt = salt;
        this.vis = vis;
    }

    public KeyType getType() {
        return type;
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

    @Override
    public boolean equals(Object o) {
        if (o == null) { return false; }
        if (o == this) { return true; }
        if (o.getClass() != getClass()) { return false; }

        BitmapKey other = (BitmapKey) o;
        return new EqualsBuilder()
                .append(type, other.type)
                .append(row, other.row)
                .append(val, other.val)
                .append(salt, other.salt)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(type.name())
                .append(row)
                .append(val)
                .append(salt)
                .toHashCode();
    }

    public int compareTo(Object o) {
        BitmapKey other = (BitmapKey) o;
        return new CompareToBuilder()
                .append(type, other.type)
                .append(row, other.row)
                .append(val, other.val)
                .append(salt, other.salt)
                .toComparison();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("type", type)
                .append("row", row)
                .append("val", val)
                .append("salt", salt)
                .toString();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(type.name());
        dataOutput.writeUTF(row);
        dataOutput.writeUTF(val);
        dataOutput.writeInt(salt);
        dataOutput.writeUTF(vis);
    }

    public void readFields(DataInput dataInput) throws IOException {
        type = KeyType.valueOf(dataInput.readUTF());
        row = dataInput.readUTF();
        val = dataInput.readUTF();
        salt = dataInput.readInt();
        vis = dataInput.readUTF();
    }
}
