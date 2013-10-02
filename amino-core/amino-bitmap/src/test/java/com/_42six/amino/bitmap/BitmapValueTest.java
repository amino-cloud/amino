package com._42six.amino.bitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class BitmapValueTest {

    @Test
    public void testReadWrite() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(outputStream);
        BitmapValue value1 = new BitmapValue();
        value1.addIndex(10);
        value1.addIndex(20);
        value1.addIndex(30);
        //value1.addBucketValue("b");
        //value1.addBucketValue("c");
        //value1.addBucketValue("y");
        value1.write(dataOutput);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput dataInput = new DataInputStream(inputStream);
        BitmapValue value2 = new BitmapValue();
        value2.readFields(dataInput);

        assertEquals(value1, value2);
    }
}
