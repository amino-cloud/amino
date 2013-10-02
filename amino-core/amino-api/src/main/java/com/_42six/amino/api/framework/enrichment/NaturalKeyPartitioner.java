package com._42six.amino.api.framework.enrichment;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<EnrichmentJoinKey,MapWritable>
{

	@Override
	public int getPartition(EnrichmentJoinKey ejk, MapWritable value, int numPartitions) {
		return (ejk.getNaturalKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
