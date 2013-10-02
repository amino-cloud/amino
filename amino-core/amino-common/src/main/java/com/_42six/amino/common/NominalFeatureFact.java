package com._42six.amino.common;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class NominalFeatureFact extends FeatureFact
{

	public NominalFeatureFact(String fact) 
	{
		super(new Text(fact));
	}

	protected NominalFeatureFact() 
	{

	}

	@Override
	public int compareTo(FeatureFact ff) 
	{
		return 1;
	}

	@Override
	public FeatureFactType getType() {
		return FeatureFactType.NOMINAL;
	}

	@Override
	public Writable setWritable(DataInput in) throws IOException
	{
		setFact(new Text(Text.readString(in)));
                
		return getFact();
	}
	
	@Override
	public String toString()
	{
		return getFact().toString();
	}

}
