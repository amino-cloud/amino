package com._42six.amino.query.services.audit;

import com._42six.amino.common.query.requests.auditing.AminoAuditRequest;

import java.io.IOException;

/**
 * Interface for an auditor service.
 */
public interface AuditorServiceInt {
	
	/**
	 * Validate the request and perform the audit
	 * @param auditRequest The information for the audit
	 * @param fireAndForget 
	 * If true: You don't have to wait for the audit. This can still
	 * throw an exception if the auditRequest is bad. However, any
	 * exceptions from submission are caught and logged.
	 * If false: You have to wait for the audit to complete. Exceptions
	 * are logged but not caught.
	 */
	public void doAudit(AminoAuditRequest auditRequest, boolean fireAndForget) throws IllegalArgumentException, IOException;
	
	/**
	 * @param user the username for the Auditor service
	 */
	public void setUser(String user);

	/**
	 * @param pass the password for the Auditor service
	 */
	public void setPass(String pass);

	/**
	 * @param production true if this is production, false if otherwise
	 * (staging, development, etc)
	 */
	public void setProduction(boolean production);
}
