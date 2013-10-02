package com._42six.amino.common.util;


public class Environment {
	
	/**
	 * Get the specification version written to the jar's amino-common
	 * manifest (if it exists).
	 * 
	 * @return the specification version
	 */
	public static String getSpecificationVersion() {
		return (new Environment()).getClass().getPackage().getSpecificationVersion();
	}
	
	/**
	 * Get the specification version written to the manifest file in
	 * this class's jar.
	 * 
	 * @param clazz the class that we want to find the version of
	 * 
	 * @return the specification version
	 */
	public static String getSpecificationVersion(Class<?> clazz) {
		return (clazz.getPackage().getSpecificationVersion());
	}
	
	/**
	 * Get the specification version written to the jar's amino-common
	 * manifest (if it exists).
	 * 
	 * @return the specification version
	 */
	public static String getImplementationVersion() {
		return (new Environment()).getClass().getPackage().getImplementationVersion();
	}
	
	/**
	 * Get the specification version written to the manifest file in
	 * this class's jar.
	 * 
	 * @param clazz the class that we want to find the version of
	 * 
	 * @return the specification version
	 */
	public static String getImplementationVersion(Class<?> clazz) {
		return (clazz.getPackage().getImplementationVersion());
	}
}
