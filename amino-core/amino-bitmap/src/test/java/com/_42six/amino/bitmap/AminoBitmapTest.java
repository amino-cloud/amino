package com._42six.amino.bitmap;


import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com._42six.amino.common.bitmap.AminoBitmap;


// Not really a test (yet), but it lets me play with bitmaps
public class AminoBitmapTest {

  @Before
  public void setUp() throws Exception {
  }

 
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testSerialization(){
    AminoBitmap bitmap = new AminoBitmap();
    
    for( int ii = 1; ii < 65000; ii+=1001 ){
      setAndPrint(bitmap, ii);
    }
  }

  @Test(expected=IndexOutOfBoundsException.class)
  public void testNegativeIndex(){
    AminoBitmap bitmap = new AminoBitmap();
      setAndPrint(bitmap, -128);
      for( Iterator<Integer> iter = bitmap.iterator(); iter.hasNext();  ){
        int bitNumber = iter.next();
        //System.out.println("Bit Set: " + bitNumber);
      }
  }
  
  @Test
  public void testIterator(){
    AminoBitmap bitmap = new AminoBitmap();
      setAndPrint(bitmap, 456);
      setAndPrint(bitmap, 12345);
      int bitCount = 0;
      for( Iterator<Integer> iter = bitmap.iterator(); iter.hasNext();  ){
        int bitNumber = iter.next();
        bitCount++;
      }
      assertEquals(bitCount, 2);
  }
  
  @Test
  public void testNotIterator(){
    AminoBitmap bitmap = new AminoBitmap();
      setAndPrint(bitmap, 456);
      setAndPrint(bitmap, 12345);
      int bitCount = 0;
      for( Iterator<Integer> iter = bitmap.notIterator(); iter.hasNext();  ){
        int bitNumber = iter.next();
        bitCount++;
      }
      assertEquals(bitCount, 12345-1);
  }
  
  @Test
  public void testToString(){
    AminoBitmap bitmap = new AminoBitmap();
    bitmap.set(1);
    bitmap.set(245);
    bitmap.set(102345);
    assertEquals("1,245,102345,", bitmap.toString());
  }


    // TODO move this test to the other packages
//  @Test
//  public void testSerialize(){
//    AminoBitmap bitmap = new AminoBitmap();
//    bitmap.set(1).set(45).set(23456541);
//    Value value = BitmapUtils.toValue(bitmap);
//    AminoBitmap bitValue = BitmapUtils.fromValue(value);
//    assertEquals("1,45,23456541,", bitmap.toString());
//    assertEquals(bitmap.toString(), bitValue.toString());
//  }
  
  private void setAndPrint(AminoBitmap bitmap, int bit){
    bitmap.set(bit);
    //System.out.println("Set Bit " + bit);
    //bitmap.printInfo();
  }
  
}
