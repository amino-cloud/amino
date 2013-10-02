package com._42six.amino.query.exception;

/**
 * Usage: When a request is issued for an entity that is not found.
 *
 */
public class EntityNotFoundException extends RuntimeException {
	public EntityNotFoundException(String s) {
		super(s);
	}
}
