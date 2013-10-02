package com._42six.amino.common;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import com._42six.amino.common.translator.FeatureFactTranslatorInt;

public class RatioFeatureFact extends IntervalFeatureFact
{

	public RatioFeatureFact(double fact) 
	{
		super(fact);
	}

	public RatioFeatureFact() 
	{

	}

	@Override
	public FeatureFactType getType() {
		return FeatureFactType.RATIO;
	}

	@Override
	public Text toText(FeatureFactTranslatorInt translator){	
		return translator.fromRatio(((DoubleWritable)fact).get());
	}
}
