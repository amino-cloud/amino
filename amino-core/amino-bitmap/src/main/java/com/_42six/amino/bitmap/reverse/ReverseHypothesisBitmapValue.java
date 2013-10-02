package com._42six.amino.bitmap.reverse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReverseHypothesisBitmapValue extends ReverseBitmapValue
{
	private String visibility;
	
	public ReverseHypothesisBitmapValue() {
		super();
	}

	public ReverseHypothesisBitmapValue(int bucketValueIndex,
			String bucketName, String bucketValue) {
		super(bucketValueIndex, bucketName, bucketValue);
	}
	
	public ReverseHypothesisBitmapValue(int bucketValueIndex,
			String bucketName, String bucketValue, String visibility) {
		super(bucketValueIndex, bucketName, bucketValue);
		
		this.visibility = visibility;
	}

	@Override
	public void readFields(DataInput input) throws IOException 
	{
		bucketValueIndex = input.readInt();
		bucketName = input.readUTF();
		bucketValue = input.readUTF();
		visibility = input.readUTF();
	}

	@Override
	public void write(DataOutput output) throws IOException 
	{
		output.writeInt(bucketValueIndex);
		output.writeUTF(bucketName);
		output.writeUTF(bucketValue);
		output.writeUTF(visibility);
	}

	public String getVisibility() {
		return visibility;
	}

	public void setVisibility(String visibility) {
		this.visibility = visibility;
	}

    public String toString(){
        return super.toString() + " Vis: " + visibility;
    }

}
