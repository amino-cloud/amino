package com._42six.amino.impl.query.services.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletWebRequest;

import javax.servlet.http.HttpServletRequest;
import java.util.HashSet;
import java.util.Set;

/**
 * SecurityService for BASIC_AUTH
 */
class BasicSecurityService implements SecurityService {

	private static final Logger logger = LoggerFactory.getLogger(BasicSecurityService.class);

	public Set<String> getVisibility() {
		final Set<String> visibility = new HashSet<>();
		visibility.add("U");
		return visibility;
	}

	public String getUserId() {
		logger.debug("BasicSecurityService get user id");
		try {
			final RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
			if (attributes != null) {
				final ServletWebRequest webRequest = (ServletWebRequest)attributes.getAttribute("org.codehaus.groovy.grails.WEB_REQUEST", RequestAttributes.SCOPE_REQUEST);

				if (webRequest != null && (HttpServletRequest.BASIC_AUTH.equals(webRequest.getRequest().getAuthType()))) {
					return webRequest.getRequest().getUserPrincipal().getName();
				}
			}
		}
		catch (Exception e) {
			logger.warn("Error extracting basic auth user from request", e);
		}
		return "NullUser";
	}

	public String getUserName() {
		return this.getUserId();
	}

	public boolean isServerCert() {
		return false;
	}

}
