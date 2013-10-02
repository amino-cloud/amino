package com._42six.amino.common;

import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import com._42six.amino.common.translator.FeatureFactTranslatorInt;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureFactTranslatorTest {
	
	final Logger logger = LoggerFactory.getLogger(FeatureFactTranslatorTest.class);
	private FeatureFactTranslatorInt translator;
	
	@Before
	public void before() {
		translator = new FeatureFactTranslatorImpl();
	}
	
	@Test @Ignore
	public void testRatioTranslator() {
		testRatio(1234.1234);
		testRatio(-1234.1234);
		testRatio(99999999);
		testRatio(9.88888888888888888888);
		testRatio(999.99999999999799);
		testRatio(9.9996978596978);
		testRatio(-99999999.999999979999);
		testRatio(0);
	}
	
	public void testRatio(double input) {
		logger.info("inputRatio: \t" + input);
		Text txtRatio = translator.fromRatio(input);
		logger.info("converted: \t" + txtRatio);
		double output = translator.toRatio(String.valueOf(txtRatio));
		logger.info("outputRatio: \t" + output);
		Assert.assertEquals(input, output, 0.0000001);
		//Assert.assertEquals(input, output, 0);
		logger.info("");
	}
	
	@Test @Ignore
	public void testIntervalTranslator() {
		testInterval(1234.1234);
		testInterval(-1234.1234);
		testInterval(99999999);
		testInterval(9.88888888888888888888);
		testInterval(999.99999999999799);
		testInterval(9.9996978596978);
		testInterval(-99999999.999999979999);
		testInterval(0);
	}
	
	public void testInterval(double input) {
		logger.info("inputIntv: \t" + input);
		Text txtInterval = translator.fromInterval(input);
		logger.info("converted: \t" + txtInterval);
		double output = translator.toInterval(String.valueOf(txtInterval));
		logger.info("outputIntv: \t" + output);
		Assert.assertEquals(input, output, 0.0000001);
		//Assert.assertEquals(input, output, 0);
		logger.info("");
	}
}
