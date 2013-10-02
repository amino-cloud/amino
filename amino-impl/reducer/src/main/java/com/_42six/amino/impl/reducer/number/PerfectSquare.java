package com._42six.amino.impl.reducer.number;

import java.util.ArrayList;

import java.lang.Math;

import com._42six.amino.api.job.AminoConfiguredReducer;
import com._42six.amino.api.job.AminoReducer;
import com._42six.amino.api.model.DatasetCollection;
import com._42six.amino.api.model.Row;
import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Feature;
import com._42six.amino.common.NominalFeatureFact;

public class PerfectSquare extends AminoConfiguredReducer implements AminoReducer
{

	private static final Feature feature = new Feature(
			"Perfect Square",
			"Whether this feature is a perfect square");

	@Override
	public Iterable<AminoWritable> reduce(DatasetCollection datasets) {
		ArrayList<AminoWritable> result = new ArrayList<AminoWritable>();
		for (Row row : datasets.getAllDatasets()) {
			String number = row.get("number");
			if (number != null && number.length() > 0) {
				Integer integer = Integer.parseInt(number);
				String perfectSquare = null;
				if (Math.pow(Math.sqrt(integer), 2) == integer) {
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
