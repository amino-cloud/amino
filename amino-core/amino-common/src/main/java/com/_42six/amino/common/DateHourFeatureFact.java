package com._42six.amino.common;

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
