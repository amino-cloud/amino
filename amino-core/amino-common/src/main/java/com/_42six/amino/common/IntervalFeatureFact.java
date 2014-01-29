package com._42six.amino.common;

import java.io.DataInput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com._42six.amino.common.translator.FeatureFactTranslatorInt;

public class IntervalFeatureFact extends FeatureFact
{

	public IntervalFeatureFact(double fact) 
	{
		super(new DoubleWritable(fact));
	}

	protected IntervalFeatureFact() 
	{

	}

    // Pass writable to super thus allowing subclasses to use something other than double
    protected IntervalFeatureFact(Writable fact)
    {
        super(fact);
    }

	@Override
	public FeatureFactType getType() {
		return FeatureFactType.INTERVAL;
	}

	@Override
	public int compareTo(FeatureFact ff) 
	{
        if(ff == null){
            return 1;
        } else {
		    return ((DoubleWritable)this.fact).compareTo(((DoubleWritable)ff.fact));
        }
	}

	@Override
	public Writable setWritable(DataInput in) throws IOException
	{
		setFact(new DoubleWritable(in.readDouble()));

		return getFact();
	}

	@Override
	public String toString()
	{
		return getFact().toString();
	}

	@Override
	public Text toText() {
		return new Text(toString());
	}

	@Override
	public Text toText(FeatureFactTranslatorInt translator) {	
		return translator.fromInterval(((DoubleWritable)fact).get());
	}

}
