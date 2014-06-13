package com._42six.amino.bitmap;

import com._42six.amino.common.AminoConfiguration;
import org.apache.hadoop.conf.Configuration;

public class BitmapConfigHelper {

  public static final String AMINO_BITMAP_INSTANCE = "amino.bitmap.instance";
  public static final String AMINO_BITMAP_ZOOKEEPERS = "amino.bitmap.zookeepers";
  public static final String AMINO_BITMAP_USERNAME = "amino.bitmap.username";
  public static final String AMINO_BITMAP_PASSWORD = "amino.bitmap.password";

  private String instance;
  private String zookeepers;
  private String username;
  private byte[] password;
  private String bucketTable;
  private String ffiTable;
  private String indexTable;
  private String featureLookupTable;
  private String jobname;
  private String inputpath;

  public BitmapConfigHelper(Configuration conf){

    // TODO - Investigate getting rid of this class all together

    instance = conf.get(AMINO_BITMAP_INSTANCE);
    zookeepers = conf.get(AMINO_BITMAP_ZOOKEEPERS);
    username = conf.get(AMINO_BITMAP_USERNAME);
    password = conf.get(AMINO_BITMAP_PASSWORD).getBytes();
    bucketTable = conf.get(AminoConfiguration.TABLE_BUCKET);
    indexTable = conf.get(AminoConfiguration.TABLE_INDEX);
    featureLookupTable = conf.get(AminoConfiguration.TABLE_FEATURE_LOOKUP);
    jobname = conf.get(AminoConfiguration.JOB_NAME);
    inputpath = conf.get(AminoConfiguration.INPUT_PATH);
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
