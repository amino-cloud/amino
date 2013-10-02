package com._42six.amino.common;

import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import com._42six.amino.common.translator.FeatureFactTranslatorInt;

import java.io.DataInput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DateHourFeatureFact extends DateFeatureFact
{
    // Number of milliseconds in one hour
    protected static long constraint = 60 * 60 * 1000;
	
    /**
     * Default constructor
     */
    protected DateHourFeatureFact() {}

    /**
     * Constructor for creating DateHours from a timestamp
     *
     * @param timeInMillis      a unix timestamp in milliseconds
     */
    public DateHourFeatureFact(long timeInMillis) {
        super(timeInMillis, constraint);
    }
	
    @Override
    public FeatureFactType getType() {
        return FeatureFactType.DATEHOUR;
    }

}
