package com._42six.amino.common;

public class MutableLong {
	private long val;
	
	public MutableLong(){
		this.val = 0;
	}
	
	public MutableLong(long initialVal){
		this.val = initialVal;
	}
	
	public MutableLong(Long initialVal){
		this.val = initialVal;
	}
	
	public void increment(){
		this.val++;
	}
	
	public void decrement(){
		this.val--;
	}
	
	public long getVal(){
		return this.val;
	}
}
