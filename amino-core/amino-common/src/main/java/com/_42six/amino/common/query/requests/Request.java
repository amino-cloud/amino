package com._42six.amino.common.query.requests;

import com._42six.amino.common.bigtable.TableConstants;

/**
 * Base class representing requests for information.  This is used to pass various confuration paramters to functions.
 * It has a verify method to ensure that all of the parameters that are needed are set.
 */
public class Request {

    public String[] securityTokens; // Any information related to security
    public String requester; // The persona that is making the request

	public Request() {
		// empty
	}

    public String getRequester(){
        return this.requester;
    }

    public void setRequester(String requester){
        this.requester = (requester.startsWith(TableConstants.USER_PREFIX)) ? requester : TableConstants.USER_PREFIX + requester;
    }

    public String[] getSecurityTokens(){
        return this.securityTokens;
    }

    public void setSecurityTokens(String[] tokens){
        this.securityTokens = tokens;
    }

	/**
	 * Method to verify if all of the parameters are set.  Should be overwritten in child classes
	 */
	public void verify() throws IllegalStateException{
		// EMPTY
	}

}
