package com._42six.amino.common.translator;

import com._42six.amino.common.FeatureFact;
import org.apache.hadoop.io.Text;

import java.math.BigDecimal;

/**
 * This implementation converts double ratios and intervals to text by:
 *   - adding a large constant value to the ratio/interval
 *   - padding the ratio/interval with zeroes on the beginning and end
 * It converts them back to double by:
 *   - subtracting the same large constant value from the ratio/interval
 * <p/>
 * This ensures both positive and negative interval/ratios are correctly
 * ascii sorted when scanned.
 */
public class FeatureFactTranslatorImpl implements FeatureFactTranslatorInt {

    public static final int CONVERT_RATIO_CONSTANT    = 100000000;
    public static final int CONVERT_INTERVAL_CONSTANT = 100000000;
    public static final BigDecimal CONVERT_RATIO_CONSTANT_BD = new BigDecimal(CONVERT_RATIO_CONSTANT);
    public static final BigDecimal CONVERT_INTERVAL_CONSTANT_BD = new BigDecimal(CONVERT_INTERVAL_CONSTANT);

    public Text fromFeatureFact(FeatureFact fact) {
        return new Text(fact.getFact().toString());
    }

    @Override
    public Text fromRatio(double fact) {
        if (fact == Double.MAX_VALUE) {
            return new Text("max");
        } else if (fact == Double.MIN_VALUE) {
            return new Text("min");
        } else {
            return new Text(String.format("%018.8f", fact + CONVERT_RATIO_CONSTANT));
        }
    }

    @Override
    public double toRatio(String fact) {
        if (fact.compareToIgnoreCase("max") == 0) {
            return Double.MAX_VALUE;
        } else if (fact.compareToIgnoreCase("min") == 0) {
            return Double.MIN_VALUE;
        } else {
            return (new BigDecimal(fact)).subtract(CONVERT_RATIO_CONSTANT_BD).doubleValue();
        }
    }

    @Override
    public Text fromInterval(double fact) {
        return new Text(String.format("%018.8f", fact + CONVERT_INTERVAL_CONSTANT));
    }

    @Override
    public double toInterval(String fact) {
        return (new BigDecimal(fact)).subtract(CONVERT_INTERVAL_CONSTANT_BD).doubleValue();
    }

    // since timestamps are never negative and specified as a whole number of milliseconds, simply
    // store longs as left justified zero padded hex.
    @Override
    public long toDate(String fact) {
        return Long.parseLong(fact, 16);
    }

    @Override
    public Text fromDate(long fact) {
        return new Text(String.format("%016X", fact));
    }
}
