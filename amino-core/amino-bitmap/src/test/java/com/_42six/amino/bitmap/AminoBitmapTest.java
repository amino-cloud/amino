package com._42six.amino.bitmap;

import com._42six.amino.common.bitmap.AminoBitmap;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

// Not really a test (yet), but it lets me play with bitmaps
public class AminoBitmapTest {

    @Test
    public void testSerialization() {
        AminoBitmap bitmap = new AminoBitmap();

        for (int ii = 1; ii < 65000; ii += 1001) {
            bitmap.set(ii);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testNegativeIndex() {
        AminoBitmap bitmap = new AminoBitmap();
        bitmap.set(-128);
    }

    @Test
    public void testIterator() {
        AminoBitmap bitmap = new AminoBitmap();
        bitmap.set(456);
        bitmap.set(12345);
        int bitCount = 0;
        for (Iterator<Integer> iter = bitmap.iterator(); iter.hasNext(); iter.next()) {
            bitCount++;
        }
        assertEquals(bitCount, 2);
    }

    @Test
    public void testNotIterator() {
        AminoBitmap bitmap = new AminoBitmap();
        bitmap.set(456);
        bitmap.set(12345);
        int bitCount = 0;
        for (Iterator<Integer> iter = bitmap.notIterator(); iter.hasNext(); iter.next() ) {
            bitCount++;
        }
        assertEquals(bitCount, 12345 - 1);
    }

    @Test
    public void testToString() {
        AminoBitmap bitmap = new AminoBitmap();
        bitmap.set(1);
        bitmap.set(245);
        bitmap.set(102345);
        assertEquals("1,245,102345,", bitmap.toString());
    }

}
