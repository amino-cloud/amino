package com._42six.amino.bitmap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class HypothesisKeyComparator extends WritableComparator 
{
	protected HypothesisKeyComparator()
	{
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) 
	{
		Text k1 = (Text)w1;
		Text k2 = (Text)w2;
		
		return k1.toString().compareTo(k2.toString());
	}
}
