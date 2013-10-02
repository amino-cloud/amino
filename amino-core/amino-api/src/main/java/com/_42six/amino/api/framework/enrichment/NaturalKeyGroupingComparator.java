package com._42six.amino.api.framework.enrichment;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator
{
	protected NaturalKeyGroupingComparator()
	{
		super(EnrichmentJoinKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) 
	{
		EnrichmentJoinKey ejk1 = (EnrichmentJoinKey)a;
		EnrichmentJoinKey ejk2 = (EnrichmentJoinKey)b;
		
		return ejk1.getNaturalKey().compareTo(ejk2.getNaturalKey());
	}
	

}
