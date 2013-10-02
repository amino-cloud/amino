package com._42six.amino.bitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class StatsKey extends BitmapKey 
{
	public String bucketName;
//	protected Map<Integer,Integer> saltedIndexes = new HashMap<Integer,Integer>();
	public int bitmapIndex;

	public StatsKey() {

    }

    public StatsKey(String row, String val, String bucketName, String vis) {
        this.type = BitmapKey.KeyType.FEATURE;
        this.row = row;
        this.val = val;
        this.bucketName = bucketName;
        this.salt = 0;
        this.vis = vis;
        
        this.bitmapIndex = 0; //doesn't matter unless doing the hypothesis job
    }
    
    public String getBucketName()
    {
    	return bucketName;
    }
    
//    public Map<Integer, Integer> getSaltedIndexes() {
//		return saltedIndexes;
//	}
//
//	public void setSaltedIndexes(Map<Integer, Integer> saltedIndexes) {
//		this.saltedIndexes = saltedIndexes;
//	}

	@Override
    public boolean equals(Object o) {
        if (o == null) { return false; }
        if (o == this) { return true; }
        if (o.getClass() != getClass()) { return false; }

        StatsKey other = (StatsKey) o;
        return new EqualsBuilder()
                .append(type, other.type)
                .append(row, other.row)
                .append(val, other.val)
                .append(bucketName, other.bucketName)
                .append(salt, other.salt)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(type.name())
                .append(row)
                .append(val)
                .append(bucketName)
                .append(salt)
                .toHashCode();
    }

    public int compareTo(Object o) {
        StatsKey other = (StatsKey) o;
        return new CompareToBuilder()
                .append(type, other.type)
                .append(row, other.row)
                .append(val, other.val)
                .append(bucketName, other.bucketName)
                .append(salt, other.salt)
                .toComparison();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("type", type)
                .append("row", row)
                .append("val", val)
                .append("bucketName", bucketName)
                .append("salt", salt)
                .toString();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(type.name());
        dataOutput.writeUTF(row);
        dataOutput.writeUTF(val);
        dataOutput.writeUTF(bucketName);
        dataOutput.writeInt(salt);
        dataOutput.writeUTF(vis);
        
//        MapWritable mw = new MapWritable();
//        Iterator<Integer> saltKeys = saltedIndexes.keySet().iterator();
//        while (saltKeys.hasNext())
//        {
//        	Integer saltKey = saltKeys.next();
//        	Integer index = saltedIndexes.get(saltKey);
//        	mw.put(new IntWritable(saltKey), new IntWritable(index));
//        }
//        mw.write(dataOutput);
        
        dataOutput.writeInt(bitmapIndex);
    }

    public void readFields(DataInput dataInput) throws IOException {
        type = KeyType.valueOf(dataInput.readUTF());
        row = dataInput.readUTF();
        val = dataInput.readUTF();
        bucketName = dataInput.readUTF();
        salt = dataInput.readInt();
        vis = dataInput.readUTF();
        
//        MapWritable mw = new MapWritable();
//        mw.readFields(dataInput);
//        saltedIndexes.clear();
//        Iterator<Writable> saltKeys = mw.keySet().iterator();
//        while (saltKeys.hasNext())
//        {
//        	IntWritable saltKey = (IntWritable)saltKeys.next();
//        	IntWritable index = (IntWritable)mw.get(saltKey);
//        	saltedIndexes.put(saltKey.get(), index.get());
//        }
        
        bitmapIndex = dataInput.readInt();
    }
}
