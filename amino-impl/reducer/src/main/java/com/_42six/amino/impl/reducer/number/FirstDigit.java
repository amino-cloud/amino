package com._42six.amino.impl.reducer.number;

import java.util.ArrayList;

import com._42six.amino.api.job.AminoConfiguredReducer;
import com._42six.amino.api.job.AminoReducer;
import com._42six.amino.api.model.DatasetCollection;
import com._42six.amino.api.model.Row;
import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Feature;
import com._42six.amino.common.RatioFeatureFact;

public class FirstDigit extends AminoConfiguredReducer implements AminoReducer
{

	private static final Feature feature = new Feature(
			"First digit", 
			"First digit of the number");

	@Override
	public Iterable<AminoWritable> reduce(DatasetCollection datasets) {
		ArrayList<AminoWritable> result = new ArrayList<AminoWritable>();
		for (Row row : datasets.getAllDatasets()) {
			String number = row.get("number");
			if (number != null && number.length() > 0) {
				result.add(new AminoWritable(feature, new RatioFeatureFact(Integer.parseInt(number.substring(0, 1)))));
			}
			else {
				System.err.println("Number is null or empty [" + number + "].");
			}
		}

		return result;
	}
}

