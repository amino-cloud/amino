package com._42six.amino.bitmap;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com._42six.amino.common.Bucket;
import com._42six.amino.common.Feature;
import com._42six.amino.common.index.BitmapIndex;

public class BitmapIndexTest {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }
  
  
  @Test
  public void testFeatureIndex(){
    Feature feature1 = new Feature("feature");
    Feature feature2 = new Feature("feature", "this feature is cool");
    Feature feature3 = new Feature("different");
    
    assertEquals(feature1.hashCode(), feature2.hashCode());
    assertFalse(feature1.hashCode() == feature3.hashCode());
  }
  /*
  @Test
  public void testFeatureResultIndex() throws IOException{
    AminoNominalResult result = new AminoNominalResult(new Feature("feature"), new Text("xyz".getBytes()));
    Bucket bucket = new Bucket("source","name","value");
    
    int featResIdx = BitmapIndex.getFeatureFactIndex(bucket, result);
    
    AminoNominalResult result2 = new AminoNominalResult(new Feature(1234), new Text("xyz".getBytes()));
   
    int featResIdx2 = BitmapIndex.getFeatureResultIndex(bucket, result2);
    
    assertFalse( featResIdx == featResIdx2);
    
  }
*/
  @Test
  public void testBucketIndex() throws IOException{
    Bucket bucket = new Bucket("source","name","value","display","visibility","HRVis");
    Bucket bucket2 = new Bucket("source","name","value2","display","visibility","HRVis");
    
    assertEquals( BitmapIndex.getBucketNameIndex(bucket), BitmapIndex.getBucketNameIndex(bucket2));
    
    Bucket bucket3 = new Bucket("source3","name","value","display","visibility","HRVis");
    assertFalse( BitmapIndex.getBucketNameIndex(bucket) == BitmapIndex.getBucketNameIndex(bucket3));

  }

  @Test
  public void testBucketValueIndex() throws IOException{
    Bucket bucket = new Bucket("source","name","value","display","visibility","HRVis");
    Bucket bucket2 = new Bucket("source","name","value2","display","visibility","HRVis");
    
    assertFalse( BitmapIndex.getBucketValueIndex(bucket) == BitmapIndex.getBucketValueIndex(bucket2));

  }
  
}
