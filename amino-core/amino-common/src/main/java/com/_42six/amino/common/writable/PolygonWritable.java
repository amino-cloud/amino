package com._42six.amino.common.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class PolygonWritable extends SpatialWritable 
{
	public int coordinateCount = 0;
	public List<CoordinateWritable> polygonCoordinates;
	
	public PolygonWritable()
	{
		
	}
	
	public PolygonWritable(List<CoordinateWritable> polygonCoordinates)
	{
		this.coordinateCount = polygonCoordinates.size();
		this.polygonCoordinates = polygonCoordinates;
	}

	@Override
	public void readFields(DataInput in) throws IOException 
	{
		this.coordinateCount = in.readInt();
		for (int i = 0; i < this.coordinateCount; i++)
		{
			this.polygonCoordinates.add(new CoordinateWritable(in.readFloat(), in.readFloat()));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		out.writeInt(this.polygonCoordinates.size());
		for (CoordinateWritable coords : this.polygonCoordinates)
		{
			coords.write(out);
		}
	}

	@Override
	public int compareTo(SpatialWritable o) 
	{
		PolygonWritable pw = (PolygonWritable)o;
		return new Float(this.polygonCoordinates.get(0).latitudey).compareTo(new Float(pw.polygonCoordinates.get(0).latitudey));
	}
	
	@Override
	public SpatialType getType() 
	{
		return SpatialType.Polygon;
	}
	
	//TODO toString() should return the geohash....

}
