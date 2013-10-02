package com._42six.amino.impl.query.services.security;

import java.util.Set;

public interface SecurityService {
	public Set<String> getVisibility();
	public String getUserId();
	public String getUserName();
	public boolean isServerCert();
}
