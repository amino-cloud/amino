package com._42six.amino.common.query.requests.bta;

import com._42six.amino.common.query.requests.ByValuesRequest;
import com._42six.amino.common.query.requests.TimedRequest;
import com._42six.amino.common.query.requests.auditing.AminoAuditRequest;

import java.util.concurrent.TimeUnit;

/**
 * 
 */
public class BtaByValuesRequest extends ByValuesRequest implements TimedRequest{
	private AminoAuditRequest auditInfo;
	private long timeout = -1; // OPTIONAL
	private TimeUnit timeoutUnits; // OPTIONAL

	/**
	 * @return the auditInfo
	 */
	public AminoAuditRequest getAuditInfo() {
		return auditInfo;
	}

	/**
	 * @param auditInfo the auditInfo to set
	 */
	public void setAuditInfo(AminoAuditRequest auditInfo) {
		this.auditInfo = auditInfo;
	}

	/**
	 * 
	 */
	public BtaByValuesRequest() {
		auditInfo = new AminoAuditRequest();
	}	

	public TimeUnit getTimeoutUnits() {
		return this.timeoutUnits;
	}

	public void setTimeoutUnits(TimeUnit units) {
		this.timeoutUnits = units;
	}

	public long getTimeout() {
		return this.timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

    @Override
    public void verify() throws IllegalStateException{
        super.verify();
        if(auditInfo == null) { throw new IllegalStateException("auditInfo is not set properly"); }
    }

}
