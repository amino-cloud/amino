package com._42six.amino.impl.reducer.number;

import com._42six.amino.api.job.AminoConfiguredReducer;
import com._42six.amino.api.job.AminoReducer;
import com._42six.amino.api.model.DatasetCollection;
import com._42six.amino.api.model.Row;
import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Feature;
import com._42six.amino.common.NominalFeatureFact;

import java.util.ArrayList;

public class HasDigitNominal extends AminoConfiguredReducer implements AminoReducer
{

	private static final Feature feature = new Feature(
			"Has digit (Nominal)", 
			"Has this digit");

	@Override
	public Iterable<AminoWritable> reduce(DatasetCollection datasets) {
		ArrayList<AminoWritable> result = new ArrayList<AminoWritable>();
		for (Row row : datasets.getAllDatasets()) {
			String number = row.get("number");
            boolean digitSeen[] = new boolean[10];
			if (number != null && number.length() > 0) {
				for (char c : number.toCharArray()) {
                    final int charVal = Character.getNumericValue(c);
                    if(digitSeen[charVal] == false){
                        digitSeen[charVal] = true;
                        result.add(new AminoWritable(feature, new NominalFeatureFact(String.valueOf(c))));
                    }
				}
			}
			else {
				System.err.println("Number is null or empty [" + number + "].");
			}
		}

		return result;
	}
}

