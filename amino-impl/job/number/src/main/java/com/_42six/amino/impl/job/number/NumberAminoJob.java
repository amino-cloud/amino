package com._42six.amino.impl.job.number;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import com._42six.amino.api.job.AminoJob;
import com._42six.amino.api.job.AminoReducer;
import com._42six.amino.api.job.JobOutputEstimate;
import com._42six.amino.data.DataLoader;
import com._42six.amino.impl.reducer.number.EvenOrOdd;
import com._42six.amino.impl.reducer.number.FirstDigit;
import com._42six.amino.impl.reducer.number.HasDigitNominal;
import com._42six.amino.impl.reducer.number.HasDigitRatio;
import com._42six.amino.impl.reducer.number.IsNumber;
import com._42six.amino.impl.reducer.number.PerfectSquare;
import com._42six.amino.impl.dataloader.number.NumberLoader;

public class NumberAminoJob implements AminoJob {

	@Override
	public Class<? extends DataLoader> getDataLoaderClass() 
	{
		return NumberLoader.class;
	}

	@Override
	public JobOutputEstimate getJobEstimate() 
	{
		return JobOutputEstimate.MEDIUM;
	}

	@Override
	public String getJobName() 
	{
		return "Number Features";
	}

	@Override
	public Iterable<Class<? extends AminoReducer>> getAminoReducerClasses() 
	{
		ArrayList<Class<? extends AminoReducer>> ars = new ArrayList<Class<? extends AminoReducer>>();
		ars.add(FirstDigit.class);
		ars.add(EvenOrOdd.class);
		ars.add(IsNumber.class);
		ars.add(HasDigitRatio.class);
		ars.add(HasDigitNominal.class);
		ars.add(PerfectSquare.class);
		return ars;
	}

	@Override
	public Integer getAminoDomainID() 
	{
		return 12345;
	}

	@Override
	public String getAminoDomainName() 
	{
		return "Numbers Domain";
	}
	
	@Override
	public String getAminoDomainDescription() 
	{
		return "Description for numbers domain";
	}

	@Override
	public void setConfig(Configuration config) {
		// TODO Auto-generated method stub
		
	}

}
