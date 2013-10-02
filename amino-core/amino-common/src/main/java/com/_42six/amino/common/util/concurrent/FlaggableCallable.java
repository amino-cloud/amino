package com._42six.amino.common.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class which handles the fact that we have to send a flag to the methods that we want to cancel because
 * Accumulo consumes the {@link InterruptedException} and thus has no way of gracefully signaling to stop.
 */
public abstract class FlaggableCallable<T> implements Callable<T>{
	protected AtomicBoolean keepWorking = new AtomicBoolean(true);
	public AtomicBoolean getKeepWorking(){ return keepWorking; }
	public void setKeepWorking(AtomicBoolean flag){keepWorking = flag;}
	
	protected String threadName;
	public String getThreadName() { return this.threadName; }
	public void setThreadName(String name) { this.threadName = name; }
	
	/**
	 * The call which implements the keepWorking flag logic
	 * @return the value of the call
	 */
	abstract protected T flaggableCall() throws Exception;
	
	/**
	 * The call to execute.  If an exception is thrown, the keepWorking  flag will be set to false and the exception will bubble up
	 */
	public T call() throws Exception{
		T returnValues = null;
		final String originalThreadName = Thread.currentThread().getName();
		if(threadName != null) {
			Thread.currentThread().setName(originalThreadName + " - " + threadName);
		}
		
		// Let the exceptions bubble up
		try{
			returnValues = flaggableCall();
		} finally {
			keepWorking.set(false);
			Thread.currentThread().setName(originalThreadName);
		} 
		
		return returnValues;
	}
}
