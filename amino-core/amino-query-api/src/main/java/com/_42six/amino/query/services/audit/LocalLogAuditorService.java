package com._42six.amino.query.services.audit;

import java.io.IOException;

import org.apache.log4j.Logger;

import com._42six.amino.common.query.requests.auditing.AminoAuditRequest;

public class LocalLogAuditorService implements AuditorServiceInt {
	
	private static final Logger log = Logger.getLogger(LocalLogAuditorService.class);

	@Override
	public void doAudit(AminoAuditRequest auditRequest, boolean fireAndForget)
			throws IllegalArgumentException, IOException {
		log.info("Logging request: " + auditRequest);
	}

	@Override
	public void setUser(String user) {
	}

	@Override
	public void setPass(String pass) {
	}

	@Override
	public void setProduction(boolean production) {
	}

}
