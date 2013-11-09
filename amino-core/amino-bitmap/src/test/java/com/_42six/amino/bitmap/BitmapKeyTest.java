package com._42six.amino.bitmap;

public class BitmapKeyTest {
/*
    @Test
    public void testEquals() throws Exception {
        BitmapKey key1 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "vis");
        BitmapKey key2 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1",  0, "vis");

        assertEquals(key1, key1);

        assertNotSame(key1, key2);
        assertEquals(key1, key2);
    }
    
    @Test
    public void testEqualsDiffVis() throws Exception {
        BitmapKey key1 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "U");
        BitmapKey key2 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "TS");

        assertEquals(key1, key1);

        assertNotSame(key1, key2);
        assertEquals(key1, key2);
    }

    @Test
    public void testHashCode() throws Exception {
        BitmapKey key1 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "vis");
        BitmapKey key2 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "vis");

        assertEquals(key1.hashCode(), key1.hashCode());

        assertNotSame(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    public void testCompareTo() throws Exception {
        BitmapKey key1 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "vis");
        BitmapKey key2 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "2", 0, "vis");

        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    public void testCompareToEquals() throws Exception {
        BitmapKey key1 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "vis");
        BitmapKey key2 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "vis");

        assertTrue(key1.compareTo(key1) == 0);
        assertTrue(key1.compareTo(key2) == 0);
        assertTrue(key2.compareTo(key1) == 0);
    }

    @Test
    public void testReadWrite() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(outputStream);
        BitmapKey key1 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "vis");
        key1.write(dataOutput);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput dataInput = new DataInputStream(inputStream);
        BitmapKey key2 = new BitmapKey();
        key2.readFields(dataInput);

        assertEquals(key1, key2);
    }

    @Test
    public void testDifferentType() throws Exception {
        BitmapKey key1 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "vis");
        BitmapKey key2 = new BitmapKey(BitmapKey.KeyType.BUCKET, "1234", "1", 0, "vis");

        assertFalse(key1.equals(key2));
        assertFalse(key1.hashCode() == key2.hashCode());
        assertFalse(key1.compareTo(key2) == 0);
    }

    @Test
    public void testDifferentRow() throws Exception {
        BitmapKey key1 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "vis");
        BitmapKey key2 = new BitmapKey(BitmapKey.KeyType.FEATURE, "5678", "1", 0, "vis");

        assertFalse(key1.equals(key2));
        assertFalse(key1.hashCode() == key2.hashCode());
        assertFalse(key1.compareTo(key2) == 0);
    }

    @Test
    public void testDifferentVal() throws Exception {
        BitmapKey key1 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "vis");
        BitmapKey key2 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "2", 0, "vis");

        assertFalse(key1.equals(key2));
        assertFalse(key1.hashCode() == key2.hashCode());
        assertFalse(key1.compareTo(key2) == 0);
    }
    
//    @Test
//    public void testDifferentBucketName() throws Exception {
//        BitmapKey key1 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "vis");
//        BitmapKey key2 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "vis");
//
//        assertFalse(key1.equals(key2));
//        assertFalse(key1.hashCode() == key2.hashCode());
//        assertFalse(key1.compareTo(key2) == 0);
//    }
    
    @Test
    public void testDifferentSalt() throws Exception {
        BitmapKey key1 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 0, "vis");
        BitmapKey key2 = new BitmapKey(BitmapKey.KeyType.FEATURE, "1234", "1", 1, "vis");

        assertFalse(key1.equals(key2));
        assertFalse(key1.hashCode() == key2.hashCode());
        assertFalse(key1.compareTo(key2) == 0);
    }*/
}
