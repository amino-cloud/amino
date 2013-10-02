package com._42six.amino.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class FeatureUtilsTest 
{
	@Test
	public void testLexDistance() 
	{
		final FeatureFactRange steveRange = new FeatureFactRange();
		steveRange.first = "Steven";
		steveRange.last = "Stephen";
		int dist = FeatureUtils.getLexicographicDistance(steveRange);
		Assert.assertTrue(dist == 6);
		
		final FeatureFactRange barrettRange = new FeatureFactRange();
		barrettRange.first = "Barret";
		barrettRange.last = "Barrett";
		dist = FeatureUtils.getLexicographicDistance(barrettRange);
		Assert.assertTrue(dist == 0);
		
		final FeatureFactRange mixRange = new FeatureFactRange();
		mixRange.first = "418999steve7^^723";
		mixRange.last = "418999steve7^^724";
		dist = FeatureUtils.getLexicographicDistance(mixRange);
		Assert.assertTrue(dist == 1);
		
		final FeatureFactRange upper = new FeatureFactRange();
		upper.first = "STEVE";
		upper.last = "steve";
		dist = FeatureUtils.getLexicographicDistance(upper);
		Assert.assertTrue(dist == 32);
		
		final FeatureFactRange garble = new FeatureFactRange();
		garble.first = "%";
		garble.last = "1";
		dist = FeatureUtils.getLexicographicDistance(garble);
		Assert.assertTrue(dist == 12);
		
		final FeatureFactRange nfl = new FeatureFactRange();
		nfl.first = "A. Rodgers";
		nfl.last = "Z. Collaros";
		dist = FeatureUtils.getLexicographicDistance(nfl);
		Assert.assertTrue(dist == 25);
		
		final ArrayList<FeatureFactRange> list = new ArrayList<FeatureFactRange>();
		list.add(steveRange);
		list.add(barrettRange);
		list.add(mixRange);
		list.add(upper);
		list.add(garble);
		list.add(nfl);
		Assert.assertTrue(FeatureUtils.getShortestDistance(list).first.equals("Barret"));
	}
}
