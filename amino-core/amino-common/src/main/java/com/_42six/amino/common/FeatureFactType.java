package com._42six.amino.common;

import com._42six.amino.common.writable.CoordinateWritable;
import com._42six.amino.common.writable.PolygonWritable;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum FeatureFactType 
{
	NOMINAL(0, Text.class),
    ORDINAL(1, Text.class),
    INTERVAL(2, DoubleWritable.class),
    RATIO(3, DoubleWritable.class),
    DATE(4, Text.class),
    DATEHOUR(5, Text.class),
    POLYGON(6, PolygonWritable.class),
    POINT(7, CoordinateWritable.class);
	
	private int code;
	private static final Map<Integer,FeatureFactType> lookup = new HashMap<Integer,FeatureFactType>();
	private Class<? extends Writable> clazz;

    public static final ImmutableSet<String> intervalTypes;
    public static final ImmutableSet<String> numericIntervalTypes;
    public static final ImmutableSet<String> dateIntervalTypes;

	static {
		for(FeatureFactType vt : EnumSet.allOf(FeatureFactType.class))
			lookup.put(vt.getCode(), vt);
            numericIntervalTypes = ImmutableSet.of(
                    INTERVAL.toString(),
                    RATIO.toString());
            dateIntervalTypes = ImmutableSet.of(
                    DATE.toString(),
                    DATEHOUR.toString());
            intervalTypes = ImmutableSet.<String>builder()
                .addAll(numericIntervalTypes)
                .addAll(dateIntervalTypes)
                .build();
	}

	private FeatureFactType(int code, Class<? extends Writable> clazz) 
	{
		this.code = code;
		this.clazz = clazz;
	}

	public int getCode() { return code; }
	
	public Class<? extends Writable> getWritable()
	{
		return clazz;
	}

	public static FeatureFactType get(int code) 
	{ 
		return lookup.get(code); 
	}
}
