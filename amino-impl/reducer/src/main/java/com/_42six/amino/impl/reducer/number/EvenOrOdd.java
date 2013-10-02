package com._42six.amino.impl.reducer.number;

import java.util.ArrayList;

import com._42six.amino.api.job.AminoConfiguredReducer;
import com._42six.amino.api.job.AminoReducer;
import com._42six.amino.api.model.DatasetCollection;
import com._42six.amino.api.model.Row;
import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Feature;
import com._42six.amino.common.NominalFeatureFact;

public class EvenOrOdd extends AminoConfiguredReducer implements AminoReducer
{

	private static final Feature feature = new Feature(
			"Even or odd", 
			"Whether this feature is even or odd");

	@Override
	public Iterable<AminoWritable> reduce(DatasetCollection datasets) {
		ArrayList<AminoWritable> result = new ArrayList<AminoWritable>();
		for (Row row : datasets.getAllDatasets()) {
			String number = row.get("number");
			if (number != null && number.length() > 0) {
				Integer integer = Integer.parseInt(number);
				String evenOdd = null;
				if (integer % 2 == 0) {
					evenOdd = "even";
				}
				else {
					evenOdd = "odd";
				}
				result.add(new AminoWritable(feature, new NominalFeatureFact(evenOdd)));
			}
			else {
				System.err.println("Number is null or empty [" + number + "].");
			}
		}

		return result;
	}
}