package com._42six.amino.common.bitmap;

import org.apache.accumulo.core.data.Value;

import java.io.*;

public class BitmapUtils {
  
  public static AminoBitmap fromValue(Value value){
    
    AminoBitmap outBitmap = new AminoBitmap();
    ByteArrayInputStream byteStream = new ByteArrayInputStream(value.get());
    DataInputStream in = new DataInputStream(byteStream);
    try {
      outBitmap.deserialize(in);
      in.close();
    } catch (IOException e) {
      throw new RuntimeException("Bitmap de-serialization error!\n"+ e.toString());
    }
    return outBitmap;
  }


  public static AminoBitmap fromValue(byte [] value){
    
    AminoBitmap outBitmap = new AminoBitmap();
    ByteArrayInputStream byteStream = new ByteArrayInputStream(value);
    DataInputStream in = new DataInputStream(byteStream);
    try {
      outBitmap.deserialize(in);
      in.close();
    } catch (IOException e) {
      throw new RuntimeException("Bitmap de-serialization error!\n"+ e.toString());
    }
    return outBitmap;
  }

  
  
  public static Value toValue(AminoBitmap bitmap){
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream( bytes );
    try {
      bitmap.serialize(out);
      out.close();
    } catch (IOException e) {
      throw new RuntimeException("Bitmap serialization error!\n"+ bitmap.toString() + "\n" + e.toString());
    }
    return new Value( bytes.toByteArray() );
  }

  public static Value getSingleBitValue(int bit){
    AminoBitmap bitmap = new AminoBitmap(bit);
    return BitmapUtils.toValue(bitmap);
  }
  
}
