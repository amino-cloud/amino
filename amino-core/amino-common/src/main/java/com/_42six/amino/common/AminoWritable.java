package com._42six.amino.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class AminoWritable implements WritableComparable<AminoWritable>
{
	protected Feature feature;
	protected FeatureFact featureFact;
	
	public AminoWritable(){}  // empty constructor required by hadoop.
	
	public AminoWritable(Feature feature, FeatureFact fact)
	{
		this.feature = feature;
		this.featureFact = fact;
	}

	@Override
	public void readFields(DataInput in) throws IOException 
	{
		this.feature = new Feature(in.readInt());
		String typeString = in.readUTF();
		FeatureFactType type = FeatureFactType.valueOf(typeString);
		this.featureFact = FeatureFactFactory.createInstance(type);
		this.featureFact.setWritable(in);
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		out.writeInt(this.feature.hashCode());
		out.writeUTF(this.featureFact.getType().name());
		this.featureFact.getFact().write(out);
	}

	@Override
	public int compareTo(AminoWritable other) 
	{
            int cmp = feature.compareTo(other.getFeature());
            if (cmp == 0) {
                return featureFact.compareTo(other.getFeatureFact());
            } else {
                return cmp;
            }
	}
	
	public Feature getFeature() 
	{
		return feature;
	}

	public FeatureFact getFeatureFact() 
	{
		return featureFact;
	}
	
	/*
	@Override
	public String toString()
	{
//		JsonObject ob = new JsonObject();
//		JsonPrimitive hash = new JsonPrimitive(this.feature.hashCode());
//		ob.add("feature-hash", hash);
//		JsonPrimitive vt = new JsonPrimitive(this.featureFact.getType().name());
//		ob.add("value-type", vt);
		
		MapWritable mw = new MapWritable();
		mw.put(new Text("feature-hash"), new IntWritable(this.feature.hashCode()));
		mw.put(new Text("fact-type"), new Text(this.featureFact.getType().name()));
		mw.put(new Text("fact"), this.featureFact.getFact());
		
		//Not sure if this will work??.....  no it doesn't ....
		//return mw.toString();
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutput dataOut = new DataOutputStream(bytesOut);
		try {
      dataOut.writeInt(this.feature.hashCode());
      dataOut.writeUTF(this.featureFact.getType().name());
      dataOut.write(this.featureFact.getFact() );
		} catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
		
	}
	
	public static AminoWritable fromHDFS(MapWritable mw)
	{
		IntWritable hash = (IntWritable)mw.get(new Text("feature-hash"));
		Feature f = new Feature(hash.get());
		
		Text type = (Text)mw.get(new Text("fact-type"));
		FeatureFactType fft = FeatureFactType.valueOf(type.toString());
		FeatureFact ff = FeatureFactFactory.createInstance(fft);
		ff.setFact(mw.get(new Text("fact")));
		
		return new AminoWritable(f, ff);
	}
*/
	
}
