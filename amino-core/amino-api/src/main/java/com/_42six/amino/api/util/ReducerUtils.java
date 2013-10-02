package com._42six.amino.api.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com._42six.amino.api.job.AminoConfiguredReducer;
import com._42six.amino.api.model.Row;
import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Feature;
import com._42six.amino.common.NominalFeatureFact;
import com._42six.amino.common.RatioFeatureFact;

/**
 * TODO: Add more reducer utils
 */
public class ReducerUtils extends AminoConfiguredReducer {

	public static double round(double valueToRound, int numberOfDecimalPlaces)
	{
		if (valueToRound == 0) {
			return 0;
		}
	    double multipicationFactor = Math.pow(10, numberOfDecimalPlaces);
	    double interestedInZeroDPs = valueToRound * multipicationFactor;
	    return Math.round(interestedInZeroDPs) / multipicationFactor;
	}
	public static long round(double a) {
		return (long)Math.floor(a + 0.5d);
	}
	
	@SuppressWarnings("serial")
	public static ArrayList<AminoWritable> createRatioFeatureFact(final Feature feature, final Integer integer) {
		return new ArrayList<AminoWritable>() {{
			add(new AminoWritable(feature, new RatioFeatureFact(integer)));
		}};
	}
	
	@SuppressWarnings("serial")
	public static ArrayList<AminoWritable> createRatioFeatureFact(final Feature feature, final Double d) {
		return new ArrayList<AminoWritable>() {{
			add(new AminoWritable(feature, new RatioFeatureFact(d)));
		}};
	}
	
	@SuppressWarnings("serial")
	public static ArrayList<AminoWritable> createNominalFeatureFact(final Feature feature, final Collection<String> stringList) {
		return new ArrayList<AminoWritable>() {{
			for (String string : stringList) {
				add(new AminoWritable(feature, new NominalFeatureFact(string)));
			}
		}};
	}
	
	@SuppressWarnings("serial")
	public static ArrayList<AminoWritable> createNominalFeatureFact(final Feature feature, final String string) {
		return new ArrayList<AminoWritable>() {{
			add(new AminoWritable(feature, new NominalFeatureFact(string)));
		}};
	}
	
	public static int getUniqueValuesInFieldCount(Collection<Row> rows, String fieldName) {
		return (getUniqueValuesInField(rows, fieldName)).size();
	}
	
	public static Set<String> getUniqueValuesInField(Collection<Row> rows, String fieldName) {
		HashSet<String> set = new HashSet<String>();
		if (rows != null) {
			for (Row row : rows) {
				String value = row.get(fieldName);
				if (value != null) {
					set.add(value);
				}
			}
		}
		return set;
	}
	
	public static Set<String> getUniqueValuesInCSVFields(Collection<Row> rows, String fieldName) {
		HashSet<String> set = new HashSet<String>();
		if (rows != null) {
			for (Row row : rows) {
				String value = row.get(fieldName);
				if (value != null) {
					String[] values = value.split(",");
					for(String singleVal : values)
						if((singleVal = singleVal.trim()) != "")
							set.add(singleVal);
				}
			}
		}
		return set;
	}
	
	public static double distanceInKm(double lat1, double lon1, double lat2, double lon2) 
	{
		double x1 = Math.toRadians(lat1);
		double y1 = Math.toRadians(lon1);
		double x2 = Math.toRadians(lat2);
		double y2 = Math.toRadians(lon2);

		double sec1 = Math.sin(x1)*Math.sin(x2);
		double dl=Math.abs(y1-y2);
		double sec2 = Math.cos(x1)* Math.cos(x2);
		
		// sec1, sec2, dl are in degree, need to convert to radians
		double centralAngle = Math.acos(sec1+sec2*Math.cos(dl));
		
		// Radius of Earth: 6378.1 kilometers
		double distance =  centralAngle * 6378.1;

		return distance;
	}
	
	public static void main(String[] args) {
		System.out.println(round(round(12345, -5)));
	}

}
