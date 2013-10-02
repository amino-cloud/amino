package com._42six.amino.common;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import com._42six.amino.common.translator.FeatureFactTranslatorInt;

public class RatioFeatureFactTest {

	RatioFeatureFact fact;
	
	@Before
	public void setup(){
		fact = new RatioFeatureFact(4.26);
	}
	
	@Ignore @Test
	public void testToTextFeatureFactTranslatorInt() {
		FeatureFactTranslatorInt translator = new FeatureFactTranslatorImpl();
		assertEquals(new Text(Double.toString(4.26)), fact.toText(translator));
	}

	@Test
	public void testGetType() {		
		assertEquals(FeatureFactType.RATIO, fact.getType());
	}

	@Test
	public void testToText() {
		Text expected = new Text(Double.toString(4.26));
		assertEquals(expected, fact.toText());
	}

	@Test
	public void testSetWritable() throws IOException {
		fact.fact = null;
		assertNull(fact.getFact());
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(os);
		out.writeDouble(4.26);
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(os.toByteArray()));		
		fact.setWritable(in);
		assertNotNull(fact.getFact());
	}

	@Test
	public void testRatioFeatureFactDouble() {
		fact = new RatioFeatureFact(1.23);
		assertEquals("1.23", fact.toString());
	}

	@Test
	public void testCompareTo() {
		assertTrue(fact.compareTo(new RatioFeatureFact(1.0)) > 0);
		assertTrue(fact.compareTo(new RatioFeatureFact(4.26)) == 0);
		assertTrue(fact.compareTo(new RatioFeatureFact(11.0)) < 0);
	}

	@Test
	public void testToString() {
		String s = fact.toString();
		assertEquals("4.26", s);
	}

	@Test
	public void testGetFact() {
		DoubleWritable dw = new DoubleWritable(4.20);
		fact.setFact(dw);
		assertEquals(dw, fact.getFact());
	}

	@Test
	public void testSetFact() {
		DoubleWritable dw = new DoubleWritable(4.20);
		fact.setFact(dw);
		assertEquals(dw, fact.getFact());
	}

}
