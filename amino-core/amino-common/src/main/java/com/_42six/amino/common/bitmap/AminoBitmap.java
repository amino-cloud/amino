package com._42six.amino.common.bitmap;

import com.googlecode.javaewah.EWAHCompressedBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class AminoBitmap implements Iterable<Integer>{  // EWAHCompressedBitmap is final class, so can't extend it

    private EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();

    public AminoBitmap(){}

    // Create a bitmap, setting a bit while doing so
    public AminoBitmap(int bit){
        this.bitmap.set(bit);
    }

    public void setBitmap(EWAHCompressedBitmap bitmap){
        this.bitmap = bitmap;
    }

    public EWAHCompressedBitmap getBitmap(){
        return this.bitmap;
    }

    /**
     * Sets the bit at a particular position.  *IMPORTANT* bits must be set in increasing order.
     * If you do not set the bits in increasing order, the set will silently fail.
     * @param bit The bit position to set
     * @return The updated bitmap
     */
    public AminoBitmap set(int bit){
        this.bitmap.set(bit);
        return this;
    }

    public void serialize(DataOutput out) throws IOException{
        this.bitmap.serialize(out);
    }

    public void deserialize(DataInput in) throws IOException{
        this.bitmap.deserialize(in);
    }

    public Iterator<Integer> iterator(){
        return bitmap.iterator();
    }

    public int cardinality(){
        return bitmap.cardinality();
    }

    public List<Integer> getPositions(){
        return bitmap.getPositions();
    }

    // creates an iterator of unset bits, up to the largest index of the set bits.
    public Iterator<Integer> notIterator(){
        final EWAHCompressedBitmap notBitmap;
        try {
            notBitmap = bitmap.clone();
            notBitmap.not();
            return notBitmap.iterator(); // memory leak?
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }


    public int sizeInBytes(){
        return this.bitmap.sizeInBytes();
    }

    public void OR(AminoBitmap other){
        this.bitmap = this.bitmap.or(other.bitmap);
    }

    public void AND(AminoBitmap other){
        this.bitmap = this.bitmap.and(other.bitmap);
    }

    public int andCardiniality(AminoBitmap other) {
        return bitmap.andCardinality(other.bitmap);
    }

    public boolean equals(AminoBitmap other){
        return this.bitmap.equals(other.bitmap);
    }

    public String toString(){
        final StringBuilder sb = new StringBuilder();
        for(Integer bitPos : this){
            sb.append(bitPos).append(",");
        }
        return sb.toString();
    }
  
}
