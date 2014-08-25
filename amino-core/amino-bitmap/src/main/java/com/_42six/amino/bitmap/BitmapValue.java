package com._42six.amino.bitmap;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;


public final class BitmapValue implements Writable {

	public static final class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Collection<Integer> ints) {
            super(IntWritable.class);
            List<IntWritable> iwl = new ArrayList<>(ints.size());
            for (Integer i : ints) {
                iwl.add(new IntWritable(i));
            }

            set(iwl.toArray(new IntWritable[iwl.size()]));
        }
    }

    private Set<Integer> indexes;

    public BitmapValue() {
    	indexes = new HashSet<>();
    }

    public BitmapValue(Integer index){
        indexes = new HashSet<>();
        indexes.add(index);
    }

    public Set<Integer> getIndexes() {
        return indexes;
    }

    public void addIndex(int index) {
        mergeIndex(Collections.singleton(index));
    }

    public void merge(BitmapValue other) {
    	mergeIndex(other.getIndexes());
    }

    public void setIndex(Integer index){
        indexes.clear();
        indexes.add(index);
    }

    private void mergeIndex(Set<Integer> others) {
        indexes.addAll(others);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(indexes)
                .toHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) { return false; }
        if (o == this) { return true; }
        if (o.getClass() != getClass()) { return false; }

        BitmapValue other = (BitmapValue) o;
        return new EqualsBuilder()
                .append(indexes, other.indexes)
                .isEquals();
    }

    @Override
    public String toString() {
    	ToStringBuilder builder = new ToStringBuilder(this);
    	builder.append("indexes", indexes);

    	return builder.toString();
    }

    public void write(DataOutput dataOutput) throws IOException {
    	IntArrayWritable i = new IntArrayWritable(indexes);
    	i.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        indexes.clear();
        IntArrayWritable i = new IntArrayWritable();
        i.readFields(dataInput);
        Set<Integer> inds = new HashSet<>();
        for (Writable w : i.get()) {
            inds.add(((IntWritable) w).get());
        }
        indexes.addAll(inds);
    }
}
