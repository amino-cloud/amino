package com._42six.amino.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Collection;

/**
 * A helpful set of utility functions to check preconditions on arguments.  This utility class supplements (@link com.google.common.base.Preconditions}.
 * 
 * NOTE: This class stole its name from a similar class in Twitter commons.
 *
 */
public class MorePreconditions {
	private final static String ARG_NOT_NULL_OR_EMPTY_DEFAULT_MESSAGE = "Arugment can not be null or empty";
	
	/*
	 * This is a class of static utility methods so we should not construct this object
	 */
	private MorePreconditions() {
	}
	
	/**
	 * Check that a string is both non-null and not empty!
	 * @param argument the argument to validate
	 * @return the argument string if it is both non-null and not empty!
	 * @throws IllegalArgumentException if the string is either null or empty!
	 */
	public static String checkNotNullOrEmpty(String argument) {
		return checkNotNullOrEmpty(argument, ARG_NOT_NULL_OR_EMPTY_DEFAULT_MESSAGE);
	}
	
	/**
	 * Check that a string is both non-null and not empty
	 * @param argument the argument to validate
	 * @param errorMessage the message template for validation exception message where %s serves as the sole arguments place holder
	 * @param args any arguments needed by the message template
	 * @return the argument if it is valid
	 * @throws IllegalArgumentException if the string is either null or empty
	 */
	public static String checkNotNullOrEmpty(String argument, String errorMessage, Object ... args) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(argument), errorMessage, args);
		return argument;
	}
	
	
	/**
	 * Check that a collection is both non-null and not empty
	 * @param argument the argument to validate
	 * @return the non null and non empty reference to a collection that was validated
	 * @throws NullPointerException if the collection is null
	 * @throws IllegealArgumentException if the collection is empty
	 */
	public static <T> Collection<T> checkNotNullOrEmpty(Collection<T> argument) {
		return checkNotNullOrEmpty(argument, ARG_NOT_NULL_OR_EMPTY_DEFAULT_MESSAGE);
	}
	
	/**
	 * Check that a collection is both non-null and not empty
	 * @param argument the argument to validate
	 * @param errorMessage the message template for validation exception message where %s serves as the sole arguments place holder
	 * @param args any arguments needed by the message template
	 * @return the argument if it is valid
	 * @throws NullPointerException if the collection is null
	 * @throws IllegealArgumentException if the collection is empty
	 */
	public static <T> Collection<T> checkNotNullOrEmpty(Collection<T> argument, String errorMessage, Object ... args) {
		Preconditions.checkNotNull(argument, errorMessage, args);
		Preconditions.checkArgument(!argument.isEmpty(), errorMessage, args);
		return argument;
	}
	
	public static <T> T[] checkNotNullOrEmpty(T [] argument, String errorMessage, Object ... args) {
		Preconditions.checkNotNull(argument);
		Preconditions.checkArgument(argument.length > 0, errorMessage, args);
		return argument;
	}
	
	/**
	 * Returns true or false depending on if the collection is empty or null 
	 * @param collection the collection to check if its empty or null
	 * @return true or false if the collection is empty or not
	 */
	public static <T> boolean isNullOrEmpty(Collection<T> collection) {
		return collection == null || collection.isEmpty();
	}
	
	
	/**
	 * Added for GROOVY
	 * Ensures the truth of an expression involving one or more parameters to the calling method.
	 *
	 * @param expression a boolean expressions
	 * @throws IllegalArgumentException - if expression is false 
	 */
	public static void checkArgument(Boolean expression) {
		Preconditions.checkArgument(expression);
	}
	
	/**
	 * Added for GROOVY
	 * Ensures the truth of an expression involving one or more parameters to the calling method.
	 * 
	 * @param expression a boolean expression
	 * @param errorMessage the exception message to use if the check fails; will be converted to a string using String.valueOf(Object)t)
	 * @throws IllegalArgumentException if the expression is false
	 */
	public static void checkArgument(Boolean expression, Object errorMessage) {
		Preconditions.checkArgument(expression, errorMessage);
	}
	
	/**
	 * Added for GROOVY
	 * Ensures the of an expression involving one or more parameters to the calling method.
	 * 
	 * @param expression a boolean expression
	 * @param errorMessageTemplate a template for the exception message should the check fail.  The
	 *					   message is formed by replacing each {@code %s} placeholder in the
	 *                     template with an argument.  These are matched by position the first
	 *                     {@code s} gets {@code errrorMessage[0]}, etc.  Unmatched arguments
	 *                     will be appened to formatted message in square braces.  Unmatched
	 *                     place holders will be left as-is.
	 * @param args the arguments to be substituted into the message template.  Arguments are
	 *             converted to strings using @link {@link String#valueOf(Object)}
	 * 
	 * @throws IllegalArgumentException if {@code expression} is false
	 * @throws NullPointerException if the check fails and either {@code errorMessageTemplate}
	 *                              or {@code errorMessageArgs} is null ( don't let this happen)
	 */
	public static void checkArugment(Boolean expression, String errorMessageTemplate, Object ... args) {
		Preconditions.checkArgument(expression, errorMessageTemplate, args);
	}
}
