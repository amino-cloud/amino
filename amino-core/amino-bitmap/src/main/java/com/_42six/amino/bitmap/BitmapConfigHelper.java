package com._42six.amino.bitmap;

import org.apache.hadoop.conf.Configuration;

public class BitmapConfigHelper {

  public static final String AMINO_BITMAP_INSTANCE = "amino.bitmap.instance";
  public static final String AMINO_BITMAP_ZOOKEEPERS = "amino.bitmap.zookeepers";
  public static final String AMINO_BITMAP_USERNAME = "amino.bitmap.username";
  public static final String AMINO_BITMAP_PASSWORD = "amino.bitmap.password";
  public static final String AMINO_BITMAP_BUCKET_TABLE = "amino.bitmap.bucketTable";
  public static final String AMINO_BITMAP_INDEX_TABLE = "amino.bitmap.indexTable";
  public static final String AMINO_FEATURE_LOOKUP_TABLE = "amino.bitmap.featureLookupTable";
  
  public static final String AMINO_BITMAP_JOB_NAME = "amino.bitmap.job.name";
  public static final String AMINO_BITMAP_INPUT_PATH = "amino.bitmap.input.path";
  
  public static final String BITMAP_CONFIG_NUM_SHARDS = "amino.bigtable.number.of.shards";
  
  private String instance;// = "nimbus_ver";
  private String zookeepers;// = "10.1.10.36:2181";
  private String username;// = "root";
  private byte[] password;// = "forkbeard";
  private String bucketTable;// = "BitmapOutput_byBucket";
  private String ffiTable;// = "BitmapOutput_byFeatureFact";
  private String indexTable;// = "BitmapOutput_bitLookup";
  private String featureLookupTable;
  private String jobname;
  private String inputpath;
  
  // setup for default localhost
  public BitmapConfigHelper(){
    instance = "accumulo";
    zookeepers = "localhost:2181";
    username = "root";
    password = "secret".getBytes();
    bucketTable = "BitmapOutput_byBucket";
    ffiTable = "BitmapOutput_byFeatureFact";
    indexTable = "BitmapOutput_bitLookup";
    featureLookupTable = "Amino_featureLookup";
    jobname = "Amino Bitmap Job";
    inputpath = "";
  }
  
  public BitmapConfigHelper( Configuration conf){
    instance = conf.get(AMINO_BITMAP_INSTANCE);
    zookeepers = conf.get(AMINO_BITMAP_ZOOKEEPERS);
    username = conf.get(AMINO_BITMAP_USERNAME);
    password = conf.get(AMINO_BITMAP_PASSWORD).getBytes();
    bucketTable = conf.get(AMINO_BITMAP_BUCKET_TABLE);
    indexTable = conf.get(AMINO_BITMAP_INDEX_TABLE);
    featureLookupTable = conf.get(AMINO_FEATURE_LOOKUP_TABLE);
    jobname = conf.get(AMINO_BITMAP_JOB_NAME);
    inputpath = conf.get(AMINO_BITMAP_INPUT_PATH);
  }

  public String instance() { return instance; }
  
  public String zookeepers(){ return zookeepers; }
  
  public String username(){ return username; }
  
  public byte[] password(){
    int len = password.length;
    byte[] copy = new byte[len];
    System.arraycopy(password, 0, copy, 0, len);
    return copy;
  }

  public String bucketTable() { return bucketTable; }
  
  public String ffiTable() { return ffiTable; }
  
  public String indexTable() { return indexTable; }
  
  public String featureLookupTable() { return featureLookupTable; }

  public String jobname() { return jobname; }

  public String inputpath() { return inputpath; }
  // delete this setter once config is all linked up and working
  public BitmapConfigHelper inputpath(String in){ this.inputpath = in; return this; }
}
