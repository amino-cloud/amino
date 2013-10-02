/**
 * 
 */
package com._42six.amino.common.query.translator;

import com._42six.amino.common.query.requests.bta.BtaByValuesRequest;
import com.google.gson.Gson;

/**
 * Translates JSON data into parameters for the methods using Gson
 */
public class GsonBtaRequestTranslator implements BtaRequestTranslator {
	static Gson gson = new Gson();
	
	/**
	 * 
	 */
	public GsonBtaRequestTranslator() {
		// EMPTY
	}
	
	public BtaByValuesRequest translateToByValuesRequest(String json){
		return  gson.fromJson(json, BtaByValuesRequest.class);
	}

}
