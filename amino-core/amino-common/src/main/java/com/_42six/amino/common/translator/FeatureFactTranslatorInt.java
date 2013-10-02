package com._42six.amino.common.translator;

import com._42six.amino.common.FeatureFact;
import org.apache.hadoop.io.Text;

public interface FeatureFactTranslatorInt {
	
	/**
	 * Translates a generic FeatureFact to Text
	 * @param fact fact to be translated
	 * @return Text
	 */
	public Text fromFeatureFact(FeatureFact fact);
	
	/**
	 * Translate a ratio double to Text
	 * @param fact double ratio value
	 * @return Text
	 */
	public Text fromRatio(double fact);
	
	/**
	 * Translate an interval double to Text
	 * @param fact double interval value
	 * @return Text
	 */
	public Text fromInterval(double fact);
	
	/**
	 * Translate a ratio string to double
	 * @param fact string ratio value
	 * @return double
	 */
	public double toRatio(String fact);
	
	/**
	 * Translate an interval string to double
	 * @param fact string interval value
	 * @return double
	 */
	public double toInterval(String fact);
        
    /**
     * Translate a date string to long
     * @param fact string date value
     * @return long
     */
    public long toDate(String fact);

    /**
     * Translate a date long to Text
     * @param fact long date value
     * @return Text
     */
    public Text fromDate(long fact);
}
