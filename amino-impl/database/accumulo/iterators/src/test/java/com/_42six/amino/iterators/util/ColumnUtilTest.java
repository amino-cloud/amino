package com._42six.amino.iterators.util;

import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.junit.Assert;
import org.junit.Test;

public class ColumnUtilTest {
	@Test
	public void basicTest() throws Exception {
		final String cf = "Hello";
		final String cq = "World";

		Column c = new Column(cf.getBytes(), cq.getBytes(), null);
		Key hasColumn = new Key("MyRow", cf, cq);
		Assert.assertTrue(ColumnUtil.keyHasColumn(hasColumn, c));
		
		Key diffColFam = new Key("MyRow", "Evil", cq);
		Assert.assertFalse(ColumnUtil.keyHasColumn(diffColFam, c));
		
		Key diffColQual = new Key("MyRow", cf, "Evil");	
		Assert.assertFalse(ColumnUtil.keyHasColumn(diffColQual, c));
	}	
}
