package com._42six.amino.api.framework.enrichment;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com._42six.amino.common.Bucket;

//Need to delete this - we aren't using this anymore.
//Want to keep it around in case my change isn't working like I think it is...
public class FrameworkEnrichmentMapper extends Mapper<Bucket, MapWritable, Bucket, MapWritable>
{

	@Override
	protected void map(Bucket key, MapWritable value, Context context) throws IOException, InterruptedException 
	{
		context.write(key, value);
	}

}
