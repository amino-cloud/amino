package com._42six.amino.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.junit.Test;

public class MorePreconditionsTest {
	private final static String NON_NULL_OR_EMPTY_STRING = "hello world!";

	@Test
	public void stringNotNullOrEmptySuccessTest() {
		assertSame(NON_NULL_OR_EMPTY_STRING,
					MorePreconditions.checkNotNullOrEmpty(NON_NULL_OR_EMPTY_STRING));
	}

	@Test(expected = IllegalArgumentException.class)
	public void stringNullFailureTest() {
		String nullString = null;
		MorePreconditions.checkNotNullOrEmpty(nullString);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void stringEmptyFailureTest() {
		String empyString = "";
		MorePreconditions.checkNotNullOrEmpty(empyString);
	}
	
	@Test
	public void stringIsEmptyWithCustomErrorMessageTest() {
		String emptyString = "";
		try {
			MorePreconditions.checkNotNullOrEmpty(emptyString, "Custom error message %s!", "1");
		} catch(IllegalArgumentException e) {
			assertEquals(e.getMessage(), "Custom error message 1!");
		}
	}
	
	@Test
	public void collectionNotNullOrEmptySuccessTest() {
		Collection<String> NON_NULL_OR_EMPTY_COLLECTION = new ArrayList<String>(1);
		NON_NULL_OR_EMPTY_COLLECTION.add(NON_NULL_OR_EMPTY_STRING);
		assertSame(NON_NULL_OR_EMPTY_COLLECTION, 
				   MorePreconditions.checkNotNullOrEmpty(NON_NULL_OR_EMPTY_COLLECTION));
	}
	
	@Test(expected = NullPointerException.class)
	public void collectionNullFailureTest() {
		Collection<String> NULL_COLLECTION = null;
		MorePreconditions.checkNotNullOrEmpty(NULL_COLLECTION);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void collectionEmptyFailureTest() {
		Collection<String> EMPTY_COLLECTION = Collections.emptyList();
		MorePreconditions.checkNotNullOrEmpty(EMPTY_COLLECTION);
	}
	
	@Test
	public void collectionNullWithCustomErrorMessage() {
		Collection<String> NULL_COLLECTION = null;
		try {
			MorePreconditions.checkNotNullOrEmpty(NULL_COLLECTION, 
					                              "Custom error message %s!", "2");
		} catch(NullPointerException e) {
			assertEquals("Custom error message 2!", e.getMessage());
		}
	}
	
	@Test
	public void collectionEmptyWithErrorMessage() {
		Collection<String> EMPTY_COLLECTION = Collections.emptyList();
		try {
			MorePreconditions.checkNotNullOrEmpty(EMPTY_COLLECTION, 
												 "Custom error message %s!", "3");
		} catch(IllegalArgumentException e) {
			assertEquals("Custom error message 3!", e.getMessage());
		}
	}
}
