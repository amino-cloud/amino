package com._42six.amino.common.query.requests;

/**
 * Base class representing requests for information.  This is used to pass various confuration paramters to functions.
 * It has a verify method to ensure that all of the parameters that are needed are set.
 */
public class Request {

    public String[] securityTokens; // Any information related to security
    public String requestor; // The persona that is making the request

	public Request() {
		// empty
	}

    public String getRequestor(){
        return this.requestor;
    }

    public void setRequestor(String requestor){
        this.requestor = requestor;
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
