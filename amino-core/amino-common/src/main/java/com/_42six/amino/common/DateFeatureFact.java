package com._42six.amino.common;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import com._42six.amino.common.translator.FeatureFactTranslatorInt;

public class DateFeatureFact extends IntervalFeatureFact
{
    // number of milliseconds to bucket features into
    protected static long constraint = 86400000;
    //protected long timeInMillis;

    /**
     * Default constructor
     */
    protected DateFeatureFact() { }

    /**
     * Constructor for timestamps
     *
     * @param timeInMillis      timestamp in milliseconds
     */
    public DateFeatureFact(long timeInMillis) {
        this(timeInMillis, constraint);
    }

    /**
     * Main constructor used for initializing the superclass fact member
     *
     * @param timeInMillis      timestamp in milliseconds
     * @param constraint        millisecond mask used to constraint the timestamp
     */
    protected DateFeatureFact(long timeInMillis, long constraint) {
        super(new FeatureFactTranslatorImpl().fromDate(constrain(timeInMillis, constraint)));
    }


    /**
     * Constructor to support instantiating from a text
     *
     * @param time      A pre-constrained, pre-translated timestamp
     */
    protected DateFeatureFact(Text time) {
        super(time);
    }

    @Override
    public Writable getFact()
    {
        return (Text)fact;
    }

    @Override
    public FeatureFactType getType()
    {
        return FeatureFactType.DATE;
    }

   @Override
   public Text toText(FeatureFactTranslatorInt translator) {
       return (Text)fact;
   }

    @Override
    public int compareTo(FeatureFact ff)
    {
        return ((Text)this.fact).compareTo(((Text)ff.fact));
    }

    @Override
    public Writable setWritable(DataInput in) throws IOException {
        setFact(new Text(Text.readString(in)));
        return getFact();
    }

    /**
     * Takes a timetsamp and returns that timestamp with some part removed
     * Constraint passed in is a number of milliseconds used to mask the timestamp
     * To constrain to one hour, pass the number of milliseconds in one hour
     *
     * @param timeInMillis      timestamp in milliseconds
     * @param constraint        number of milliseconds to strip of timestamp
     *
     * @return                  the time in milliseconds with constraint removed
     */
    protected static long constrain(long timeInMillis, long constraint)
    {
        return timeInMillis - (timeInMillis % constraint);
    }
}
