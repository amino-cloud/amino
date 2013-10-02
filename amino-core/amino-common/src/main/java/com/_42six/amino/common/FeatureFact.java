package com._42six.amino.common;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com._42six.amino.common.translator.FeatureFactTranslatorInt;

public abstract class FeatureFact implements Comparable<FeatureFact>
{
	protected Writable fact;

	public FeatureFact(Writable fact)
	{
		this.fact = fact;
	}

	public FeatureFact()
	{

	}

	public Text toText(){
		return new Text(this.fact.toString());
	}

	public Text toText(FeatureFactTranslatorInt translator) {
		return translator.fromFeatureFact(this);
	}

	public Writable getFact() {
		return fact;
	}

	public abstract FeatureFactType getType();

	public void setFact(Writable fact) {
		this.fact = fact;
	}

	public abstract Writable setWritable(DataInput in) throws IOException;
}
