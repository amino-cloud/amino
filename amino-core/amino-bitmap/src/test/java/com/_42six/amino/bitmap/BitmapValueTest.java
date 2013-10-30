package com._42six.amino.bitmap;

import org.junit.Test;

import java.io.*;

import static junit.framework.Assert.assertEquals;

public class BitmapValueTest {

    @Test
    public void testReadWrite() throws Exception {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DataOutput dataOutput = new DataOutputStream(outputStream);
        final BitmapValue value1 = new BitmapValue();
        value1.addIndex(10);
        value1.addIndex(20);
        value1.addIndex(30);
        value1.write(dataOutput);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final DataInput dataInput = new DataInputStream(inputStream);
        final BitmapValue value2 = new BitmapValue();
        value2.readFields(dataInput);

        assertEquals(value1, value2);
    }
}
