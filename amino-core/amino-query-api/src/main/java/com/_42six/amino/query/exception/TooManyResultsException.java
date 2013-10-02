package com._42six.amino.query.exception;

/**
 * Usage: When a request gets too many results.
 *
 */
public class TooManyResultsException extends RuntimeException {
	public TooManyResultsException(String s) {
		super(s);
	}
}
