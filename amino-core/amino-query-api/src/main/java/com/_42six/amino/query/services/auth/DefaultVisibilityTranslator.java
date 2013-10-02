package com._42six.amino.query.services.auth;

import java.util.Iterator;
import java.util.Set;

public class DefaultVisibilityTranslator implements VisibilityTranslatorInt {
	
	@Override
	public String combineHumanReadable(Set<String> visibilities) throws Exception {
		StringBuilder outputVisibility = new StringBuilder();
		Iterator<String> visIterator = visibilities.iterator();
		while (visIterator.hasNext()) {
			outputVisibility.append(visIterator.next());
			if (visIterator.hasNext()) {
				outputVisibility.append(",");
			}
		}
		return outputVisibility.toString();
	}
}
