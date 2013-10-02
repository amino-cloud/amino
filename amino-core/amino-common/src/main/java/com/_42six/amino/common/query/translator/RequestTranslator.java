/**
 * 
 */
package com._42six.amino.common.query.translator;

import com._42six.amino.common.query.requests.ByValuesRequest;

/**
 * Translates data into the params needed for the methods
 */
public interface RequestTranslator {
	 public ByValuesRequest  translateToByValuesRequest(String json);
}
