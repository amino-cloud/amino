package com._42six.amino.common.index;

import com._42six.amino.common.Bucket;
import com._42six.amino.common.BucketStripped;
import com._42six.amino.common.Feature;
import com._42six.amino.common.FeatureFact;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.Hash;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public class BitmapIndex {

    private static final int HASH_TYPE = Hash.MURMUR_HASH;
    private static final int EWAH_DIFFERENCE = 64; // TODO - look at getting this from EWAH wordinbits
    private static final int MAX_EWAH = Integer.MAX_VALUE - EWAH_DIFFERENCE;

    /**
     * Hack for EWAH since it can't handle set'ing a position greater than MAX_EWAH.  This will lead to some extra potential
     * collisions, but hey, at least it works.
     */
    private static int getEwah(int position){
        if(position == Integer.MIN_VALUE){
            return 0;
        }

        int normalized = Math.abs(position);

        if(normalized > MAX_EWAH){
            return normalized - EWAH_DIFFERENCE;
        } else {
            return normalized;
        }
    }

    private static int getBucketNameIndex(String dataSource, String name, int seed) {
        Hash hasher = Hash.getInstance(HASH_TYPE);
        int hashcode = hasher.hash(dataSource.getBytes(), seed);
        hashcode = hasher.hash(name.getBytes(), hashcode);
        return getEwah(hashcode);
    }
    
    public static int getBucketNameIndex(String dataSource, String name) {
        return getBucketNameIndex(dataSource, name, -1);
    }
    
    private static int getBucketNameIndex(Bucket bucket, int seed) {
        return getBucketNameIndex(bucket.getBucketDataSource().toString(), bucket.getBucketName().toString(), seed);
    }

    public static int getBucketNameIndex(Bucket bucket) {
        return getBucketNameIndex(bucket, -1);
    }

    public static int getFeatureIndex(Feature feature) {
        int hashcode = feature.hashCode();
        return getEwah(hashcode);
    }

    /*public static int getFeatureNameIndex(String name) {
        Hash hasher = Hash.getInstance(HASH_TYPE);
        return Math.abs(hasher.hash(name.getBytes()));
    }*/

    private static int getFeatureFactIndex(Bucket bucket, Feature feature, FeatureFact fact) {
        return getFeatureFactIndex(bucket, feature, fact, -1);
    }

    public static int getFeatureFactIndex(Bucket bucket, Feature feature, FeatureFact fact, int seed) {
        Hash hasher = Hash.getInstance(HASH_TYPE);

        int hashcode = BitmapIndex.getBucketNameIndex(bucket, seed);
        hashcode = hasher.hash(Integer.toString(getFeatureIndex(feature)).getBytes(), hashcode);

        byte[] factBytes = getFeatureFactBytes(fact);
        hashcode = hasher.hash(factBytes, hashcode); // XXX value.getBytes()? need to see where value ends up

        return getEwah(hashcode);
    }

    public static int getBucketValueIndex(Bucket bucket) {
        Hash hasher = Hash.getInstance(HASH_TYPE);
        int hashcode = getBucketNameIndex(bucket);
        hashcode = hasher.hash(bucket.getBucketValue().toString().getBytes(), hashcode);
        return getEwah(hashcode);
    }
    
    public static int getBucketValueIndex(BucketStripped bucketStripped) {
    	Hash hasher = Hash.getInstance(HASH_TYPE);
    	return getEwah(hasher.hash(bucketStripped.getBucketValue().toString().getBytes(), bucketStripped.getCacheHash().get()));
    }
    
    public static int getBucketCacheIndex(Bucket bucket) {
    	return getBucketNameIndex(bucket);
    	//TODO: include visibility?
    }

    /**
     * Returns the bitmap position for a given BucketName/BucketValue/seed
     * @param bucket The bucket name and value
     * @param seed The salt that the return value is valid for
     * @return The index in a bitmap for the given salt
     */
    public static int getValueIndex(Bucket bucket, int seed)
    {
    	//This is just the bucketValue only, no need for datasource
        Hash hasher = Hash.getInstance(HASH_TYPE);
        int hashcode = hasher.hash(bucket.getBucketName().toString().getBytes(), seed);
        hashcode = hasher.hash(bucket.getBucketValue().getBytes(), hashcode);
        return getEwah(hashcode);
    }

    private static byte[] getFeatureFactBytes(FeatureFact ff) {
        Writable w = ff.getFact();
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        DataOutput dataOut = new DataOutputStream(outBytes);
        try {
            w.write(dataOut);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return outBytes.toByteArray();
    }
}
