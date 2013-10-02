package com._42six.amino.impl.reducer.number;

import java.util.ArrayList;

import com._42six.amino.api.job.AminoConfiguredReducer;
import com._42six.amino.api.job.AminoReducer;
import com._42six.amino.api.model.DatasetCollection;
import com._42six.amino.api.model.Row;
import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Feature;
import com._42six.amino.common.RatioFeatureFact;

public class IsNumber extends AminoConfiguredReducer implements AminoReducer
{
	private static final Feature feature = new Feature(
			"Is Number", 
			"Is Number");

	@Override
	public Iterable<AminoWritable> reduce(DatasetCollection datasets) {
		ArrayList<AminoWritable> result = new ArrayList<AminoWritable>();
		for (Row row : datasets.getAllDatasets()) {
			String number = row.get("number");
			if (number != null && number.length() > 0) {
				result.add(new AminoWritable(feature, new RatioFeatureFact(Integer.parseInt(number))));
			}
			else {
				System.err.println("Number is null or empty [" + number + "].");
			}
		}
		return result;
	}
}

