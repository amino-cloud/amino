package com._42six.amino.common;
//import static org.junit.Assert.assertEquals;

//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.DataInputStream;
//import java.io.DataOutputStream;
import java.io.IOException;
//import java.nio.ByteBuffer;

//import org.apache.hadoop.io.Text;
import org.junit.Test;


public class BucketTest {

	/*
	private String toHexString(final ByteBuffer hash) {
		byte [] byteArray = hash.array();
		StringBuffer hexString = new StringBuffer();
		for(int i=0; i < byteArray.length; i++) {
			hexString.append(Integer.toHexString(0XFF & byteArray[i]));
		}
		return hexString.toString();
	}
	*/
	
	@Test
	public void testBucketBasic() throws IOException {
		/*
		Bucket myBucket = new Bucket(new Text("test_data_source"), new Text("shopper_id_bucket"));
		// Verify that the hashes match
		assertEquals("453ec3aa63347983d45bbe15b5561d058baaf6", this.toHexString(myBucket.getHash()));
		*/
	}
	
	@Test
	public void testBucketSerializationDeserialization() throws IOException {
		/*
		// Reference bucket
		Bucket myBucket = new Bucket(new Text("test_data_source"), new Text("shopper_id_bucket"));

		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		myBucket.write(new DataOutputStream(baos));
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		Bucket otherBucket = new Bucket();
		otherBucket.readFields(new DataInputStream(bais));
		
		assertEquals(0, myBucket.compareTo(otherBucket));
		*/
	}
}
