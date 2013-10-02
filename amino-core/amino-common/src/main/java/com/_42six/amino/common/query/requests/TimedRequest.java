package com._42six.amino.common.query.requests;

import java.util.concurrent.TimeUnit;

public interface TimedRequest {
	public long getTimeout();
	public void setTimeout(long timeout);
	public TimeUnit getTimeoutUnits();
	public void setTimeoutUnits(TimeUnit units);
}
