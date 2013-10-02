package com._42six.amino.common;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import com._42six.amino.common.translator.FeatureFactTranslatorInt;

public class DateFeatureFactTest {

        DateFeatureFact fact;
	
	@Before
	public void setup(){
		fact = new DateFeatureFact(1056206458000L);
	}

        @Test
        public void testWritable() {
            FeatureFactTranslatorInt translator = new FeatureFactTranslatorImpl();
            assertEquals(1056153600000L, translator.toDate(fact.toText().toString()));
        }
	
	@Ignore @Test
	public void testToTextFeatureFactTranslatorInt() {
		FeatureFactTranslatorInt translator = new FeatureFactTranslatorImpl();
		assertEquals(new Text("000000004D3F6400"), fact.toText(translator));
	}

	@Test
	public void testGetType() {		
		assertEquals(FeatureFactType.DATE, fact.getType());
	}

	@Ignore @Test
	public void testToText() {
		Text expected = new Text(Double.toString(4.26));
		assertEquals(expected, fact.toText());
	}

	@Ignore @Test
	public void testSetWritable() throws IOException {
		fact.fact = null;
		assertNull(fact.getFact());
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(os);
		out.writeLong(1372251476L);
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(os.toByteArray()));		
		fact.setWritable(in);
		assertNotNull(fact.getFact());
	}

	@Test
	public void testDateFeatureFactDouble() {
		//fact = new DateFeatureFact(1.23);
		//assertEquals("1.23", fact.toString());
	}

	@Test
	public void testCompareTo() {
		//assertTrue(fact.compareTo(new DateFeatureFact(1.0)) > 0);
		//assertTrue(fact.compareTo(new DateFeatureFact(4.26)) == 0);
		//assertTrue(fact.compareTo(new DateFeatureFact(11.0)) < 0);
	}

	@Test
	public void testToString() {
		//String s = fact.toString();
		//assertEquals("4.26", s);
	}

	@Test
	public void testGetFact() {
		//DoubleWritable dw = new DoubleWritable(4.20);
		//fact.setFact(dw);
		//assertEquals(dw, fact.getFact());
	}

	@Test
	public void testSetFact() {
		//DoubleWritable dw = new DoubleWritable(4.20);
		//fact.setFact(dw);
		//assertEquals(dw, fact.getFact());
	}

}
