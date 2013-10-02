package com._42six.amino.common;

import java.io.DataInput;
import java.io.IOException;
import com._42six.amino.common.FeatureFact;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


//public class OrdinalFeatureFact<E extends Enum<E>> extends FeatureFact
public class OrdinalFeatureFact extends FeatureFact
{

	//public OrdinalFeatureFact(E factEnum) 
	public OrdinalFeatureFact(int index, String value)
	{
		//super(new EnumWritable<E>(factEnum));
		MapWritable mw = new MapWritable();
		mw.put(new IntWritable(index), new Text(value));
	}

	protected OrdinalFeatureFact() 
	{

	}
        
	@Override
	public int compareTo(FeatureFact ff)
	{
		//return ((EnumWritable<E>)this.fact).getValue().compareTo(((EnumWritable<E>)ff.fact).getValue());
		int myKey = ((IntWritable)((MapWritable)this.fact).keySet().iterator().next()).get();
		int otherKey = ((IntWritable)((MapWritable)ff.fact).keySet().iterator().next()).get();
		return new Integer(myKey).compareTo(new Integer(otherKey));
	}
	
	@Override
	public FeatureFactType getType() 
	{
		return FeatureFactType.ORDINAL;
	}
	
	@Override
	public Writable setWritable(DataInput in) throws IOException
	{
		//EnumWritable<E> ew = new EnumWritable<E>();
		//ew.readFields(in);
		MapWritable mw = new MapWritable();
		mw.readFields(in);
		setFact(mw);
                
		return getFact();
	}
	

	@Override
	public String toString()
	{
		//return ((EnumWritable<E>)this.fact).getValue().name();
		IntWritable key = ((IntWritable)((MapWritable)this.fact).keySet().iterator().next());
		Text value = (Text)((MapWritable)this.fact).get(key);
		return key.toString() + ":" + value.toString();
	}
}
