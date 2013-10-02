/**
 * 
 */
package com._42six.amino.common.util.concurrent;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TimedCallInfo {
	long timeout = -1;
	TimeUnit units;
	String user;
	boolean mayInterruptIfRunning = false;

    /**
	 * @return the units
	 */
	public TimeUnit getUnits() {
		return units;
	}

	/**
	 * @param units the units to set
	 */
	public void setUnits(TimeUnit units) {
		this.units = units;
	}

	/**
	 * @return the timeout
	 */
	public long getTimeout() {
		return timeout;
	}

	/**
	 * @param timeout the timeout to set
	 */
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @param user the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * @return the mayInterruptIfRunning
	 */
	public boolean isMayInterruptIfRunning() {
		return mayInterruptIfRunning;
	}

	/**
	 * @param mayInterruptIfRunning the mayInterruptIfRunning to set
	 */
	public void setMayInterruptIfRunning(boolean mayInterruptIfRunning) {
		this.mayInterruptIfRunning = mayInterruptIfRunning;
	}

	/**
	 * 
	 */
	public TimedCallInfo() {
		// Empty
	}
	
}
