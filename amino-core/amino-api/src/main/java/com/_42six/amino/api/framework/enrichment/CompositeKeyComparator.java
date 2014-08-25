package com._42six.amino.api.framework.enrichment;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator 
{
	protected CompositeKeyComparator()
	{
		super(EnrichmentJoinKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		EnrichmentJoinKey k1 = (EnrichmentJoinKey)w1;
		EnrichmentJoinKey k2 = (EnrichmentJoinKey)w2;
		
		int result = k1.getNaturalKey().compareTo(k2.getNaturalKey());
		if(0 == result) {
			result = new Integer(k1.getType()).compareTo(k2.getType());
			if (0 == result && k1.getType() == EnrichmentJoinKey.TYPE_SUBJECT)
			{
				result = k1.getBucket().compareTo(k2.getBucket());
			}
		}
		return result;
	}
}

