package com._42six.amino.common.exception;

public class LocalFileServiceException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public LocalFileServiceException(Exception e) {
		super(e);
	}
	
	public LocalFileServiceException(String message) {
		super(message);
	}
	
	public LocalFileServiceException(String message, Exception e) {
		super(message, e);
	}
}
