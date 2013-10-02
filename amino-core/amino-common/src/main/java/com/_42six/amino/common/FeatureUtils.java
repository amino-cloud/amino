package com._42six.amino.common;

public class FeatureUtils 
{
	public static FeatureFactRange getShortestDistance(Iterable<FeatureFactRange> ranges)
	{
		int smallestDist = Integer.MAX_VALUE;
		FeatureFactRange smallest = null;
		for (FeatureFactRange ffr : ranges)
		{
			int dist = getLexicographicDistance(ffr);
			if (dist < smallestDist) {
				smallest = ffr;
				smallestDist = dist;
			}
		}
		
		return smallest;
	}
	
	public static int getLexicographicDistance(FeatureFactRange range)
	{
		char[] first = range.first.toCharArray();
		char[] last = range.last.toCharArray();
		int length = 0;
		if (first.length < last.length)
		{
			length = first.length;
		}
		else
		{
			length = last.length;
		}
		
		int dist = 0;
		int index = 0;
		while (dist == 0 && index < length)
		{
			dist = getDistanceAtCharacter(first, last, index);
			index++;
		}
		
		return dist;
	}
	
	private static int getDistanceAtCharacter(char[] first, char[] last, int index)
	{
		int firstVal = first[index];
		int lastVal = last[index];
		return Math.abs(lastVal - firstVal);
	}
}
