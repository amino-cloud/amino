package com._42six.amino.common.writable;

import org.apache.hadoop.io.WritableComparable;

public abstract class SpatialWritable implements WritableComparable<SpatialWritable>
{

	public abstract SpatialType getType();
}
