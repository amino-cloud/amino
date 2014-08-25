package com._42six.amino.common;

import com._42six.amino.common.writable.CoordinateWritable;
import com._42six.amino.common.writable.PolygonWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PolygonFeatureFact extends FeatureFact
{
	protected PolygonFeatureFact()
	{
		
	}
	
	public PolygonFeatureFact createInstance(List<PointFeatureFact> orderedPoints)
	{
		ArrayList<CoordinateWritable> coords = new ArrayList<>();
		for (PointFeatureFact pff : orderedPoints)
		{
			coords.add((CoordinateWritable)pff.getFact());
		}
		return new PolygonFeatureFact(coords);
	}
	
	private PolygonFeatureFact(List<CoordinateWritable> polygonCoordinates)
	{
		super(new PolygonWritable(polygonCoordinates));
	}

	@Override
	public Writable getFact() 
	{
		return (PolygonWritable)this.fact;
	}

	@Override
	public FeatureFactType getType() 
	{
		return FeatureFactType.POLYGON;
	}

	@Override
	public void setFact(Writable fact) 
	{
		this.fact = (PolygonWritable)fact;
	}

	@Override
	public Writable setWritable(DataInput in) throws IOException 
	{
		PolygonWritable pw = new PolygonWritable();
		pw.readFields(in);
		this.fact = pw;
		return this.fact;
	}

	@Override
	public int compareTo(FeatureFact ff) 
	{
		return ((PolygonWritable)this.fact).compareTo(((PolygonWritable)ff.fact));
	}
	
	@Override
	public String toString()
	{
		return "I'm a polygon...still need to do this...";
	}

}
