package com._42six.amino.common.index;

import com._42six.amino.common.*;
import org.apache.hadoop.io.Text;
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
    private static final Hash hasher = Hash.getInstance(HASH_TYPE);

    /**
     * Hack for EWAH since it can't handle set'ing a position greater than MAX_EWAH.  This will lead to some extra potential
     * collisions, but hey, at least it works.
     */
    private static int getEwah(int position){
        // The majority of time it should be the first case as it's already supposed to be Math.abs'd
        if(position >= 0 && position <= MAX_EWAH){
            return position;
        } else if (position == Integer.MIN_VALUE){
            return 0;
        } else if (position > MAX_EWAH){
            return position - EWAH_DIFFERENCE;
        } else {
            int normalize = Math.abs(position);
            if(normalize <= MAX_EWAH){
                return normalize;
            } else {
                return normalize - EWAH_DIFFERENCE;
            }
        }
    }

    private static int getBucketNameIndex(String dataSource, String name, int seed) {
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

    private static int getFeatureFactIndex(Bucket bucket, Feature feature, FeatureFact fact) {
        return getFeatureFactIndex(bucket, feature, fact, -1);
    }

    public static int getFeatureFactIndex(Bucket bucket, Feature feature, FeatureFact fact, int seed) {
        int hashcode = BitmapIndex.getBucketNameIndex(bucket, seed);
        hashcode = hasher.hash(Integer.toString(getFeatureIndex(feature)).getBytes(), hashcode);

        byte[] factBytes = getFeatureFactBytes(fact);
        hashcode = hasher.hash(factBytes, hashcode); // XXX value.getBytes()? need to see where value ends up

        return getEwah(hashcode);
    }

    public static int getBucketValueIndex(Bucket bucket) {
        int hashcode = getBucketNameIndex(bucket);
        hashcode = hasher.hash(bucket.getBucketValue().toString().getBytes(), hashcode);
        return getEwah(hashcode);
    }
    
    public static int getBucketValueIndex(BucketStripped bucketStripped) {
    	return getEwah(hasher.hash(TextUtils.getBytes(bucketStripped.getBucketValue()), bucketStripped.getCacheHash().get()));
    }

    public static int getBucketValueIndex(Text bucketValue, int cacheHash){
        return getEwah(hasher.hash(TextUtils.getBytes(bucketValue), cacheHash));
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
    	// This is just the bucketValue only, no need for datasource
        int hashcode = hasher.hash(bucket.getBucketName().toString().getBytes(), seed);
        hashcode = hasher.hash(TextUtils.getBytes(bucket.getBucketValue()), hashcode);
        return getEwah(hashcode);
    }

    private static byte[] getFeatureFactBytes(FeatureFact ff) {
        final Writable w = ff.getFact();
        final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        final DataOutput dataOut = new DataOutputStream(outBytes);
        try {
            w.write(dataOut);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return outBytes.toByteArray();
    }
}
