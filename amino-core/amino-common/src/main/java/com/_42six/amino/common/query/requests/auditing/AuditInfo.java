/**
 * 
 */
package com._42six.amino.common.query.requests.auditing;

/**
 * Provides information for auditing purposes
 */
public class AuditInfo implements Auditable{

	/**
	 * Reason for why a query is being made
	 */
	String justification; 
	
	/**
	 * The person that the request is being made on the behalf of
	 */
	String requester;
	
	/**
	 * @return the requester
	 */
	public String getRequester() {
		return requester;
	}

	/**
	 * @param requester the requester to set
	 */
	public void setRequester(String requester) {
		this.requester = requester;
	}

	@Override
	public String getJustification() {
		return this.justification;
	}

	@Override
	public void setJustifcation(String justification) {
		this.justification = justification;
	}
	
	/**
	 * Information for auditing purposes
	 */
	public AuditInfo() {
		// EMPTY
	}
	
}
