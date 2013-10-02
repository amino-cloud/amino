/**
 * 
 */
package com._42six.amino.common.query.translator;

import com._42six.amino.common.query.requests.bta.BtaByValuesRequest;

/**
 * 
 */
public interface BtaRequestTranslator extends RequestTranslator {
	public BtaByValuesRequest  translateToByValuesRequest(String json);
}
