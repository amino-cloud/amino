package com._42six.amino.common.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class EnumWritable<E extends Enum<E>> implements Writable 
{
	private Class<E> cls;
    private E value;
    
    public EnumWritable()
    {
    	
    }
    
    @SuppressWarnings("unchecked")
    public EnumWritable(E value) 
    {
        this.cls = (Class<E>) value.getClass();
        this.value = value;
    }
    
    public E getValue() 
    {
        return value;
    }

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput input) throws IOException 
	{
		String className = input.readUTF();
		if (this.cls == null)
		{
			try 
			{
				this.cls = (Class<E>)Class.forName(className);
			} 
			catch (ClassNotFoundException e) 
			{
				e.printStackTrace();
			}
		}
		value = WritableUtils.readEnum(input, cls);
	}

	@Override
	public void write(DataOutput output) throws IOException 
	{
		output.writeUTF(this.cls.getName());
        WritableUtils.writeEnum(output, value);
    }

}
