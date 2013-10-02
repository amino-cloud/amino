package com._42six.amino.query.services.auth;

import java.util.Set;

public interface VisibilityTranslatorInt {
	
	public String combineHumanReadable(final Set<String> visibilities) throws Exception;
}
