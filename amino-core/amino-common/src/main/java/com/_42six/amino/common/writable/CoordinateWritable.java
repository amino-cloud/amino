package com._42six.amino.common.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CoordinateWritable extends SpatialWritable
{
	public double longitudex;
	public double latitudey;
	
	public CoordinateWritable()
	{
		
	}
	
	public CoordinateWritable(double longitudex, double latitudey)
	{
		this.longitudex = longitudex;
		this.latitudey = latitudey;
	}

	@Override
	public void readFields(DataInput in) throws IOException 
	{
		this.longitudex = in.readDouble();
		this.latitudey = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		out.writeDouble(this.longitudex);
		out.writeDouble(this.latitudey);
	}

	@Override
	public int compareTo(SpatialWritable o) 
	{
		CoordinateWritable cw = (CoordinateWritable)o;
		return new Double(this.latitudey).compareTo(cw.latitudey);
	}

	@Override
	public SpatialType getType() 
	{
		return SpatialType.Coordinate;
	}
	
	//TODO toString() should return the geohash....

}
