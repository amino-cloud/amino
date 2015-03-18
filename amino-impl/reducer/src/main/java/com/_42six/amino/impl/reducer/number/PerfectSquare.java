package com._42six.amino.impl.reducer.number;

import com._42six.amino.api.job.AminoConfiguredReducer;
import com._42six.amino.api.job.AminoReducer;
import com._42six.amino.api.model.DatasetCollection;
import com._42six.amino.api.model.Row;
import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Feature;
import com._42six.amino.common.NominalFeatureFact;

import java.util.ArrayList;

public class PerfectSquare extends AminoConfiguredReducer implements AminoReducer
{

	private static final Feature feature = new Feature(
			"Perfect Square",
			"Whether this feature is a perfect square");

    private static boolean isPerfectSquare(long n)
    {
        if (n < 0)
            return false;

        switch((int)(n & 0x3F))
        {
            case 0x00: case 0x01: case 0x04: case 0x09: case 0x10: case 0x11:
            case 0x19: case 0x21: case 0x24: case 0x29: case 0x31: case 0x39:
            long sqrt;
            if(n < 410881L)
            {
                //John Carmack hack, converted to Java.
                // See: http://www.codemaestro.com/reviews/9
                int i;
                float x2, y;

                x2 = n * 0.5F;
                y  = n;
                i  = Float.floatToRawIntBits(y);
                i  = 0x5f3759df - ( i >> 1 );
                y  = Float.intBitsToFloat(i);
                y  = y * ( 1.5F - ( x2 * y * y ) );

                sqrt = (long)(1.0F/y);
            }
            else
            {
                //Carmack hack gives incorrect answer for n >= 410881.
                sqrt = (long)Math.sqrt(n);
            }
            return sqrt*sqrt == n;

            default:
                return false;
        }
    }

	@Override
	public Iterable<AminoWritable> reduce(DatasetCollection datasets) {
		ArrayList<AminoWritable> result = new ArrayList<AminoWritable>();
		for (Row row : datasets.getAllDatasets()) {
			String number = row.get("number");
			if (number != null && number.length() > 0) {
				Integer integer = Integer.parseInt(number);
				String perfectSquare;
				if (isPerfectSquare(integer)) {
					perfectSquare = "perfect square";
				}
				else {
					perfectSquare = "not square";
				}
				result.add(new AminoWritable(feature, new NominalFeatureFact(perfectSquare)));
			}
			else {
				System.err.println("Number is null or empty [" + number + "].");
			}
		}

		return result;
	}
}
