package com._42six.amino.query.services.accumulo;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Class for containing values for configuring Scan iterators
 */
public class ScanIteratorConfig {

    /** The priority of the iterator.  Usually set to 30 */
	int priority;

    /** The iterator class.  Usually getCanonicalName() */
	String iteratorClass;

    /** String represtation of the class */
	String name;

    /** Any options associated with the iterator */
	Set<ScanIteratorOption> options;

	/**
	 * Empty Constructor
	 */
	public ScanIteratorConfig(){
		// EMPTY
	}

	public ScanIteratorConfig(int priority, String iteratorClass, String name, Set<ScanIteratorOption> options) {
		this.priority = priority;
		this.iteratorClass = iteratorClass;
		this.name = name;
		this.options = options;
	}

	/**
	 * Copy constructor
	 * @param that ScanIteratorConfig to copy
	 */
	public ScanIteratorConfig(ScanIteratorConfig that){
		this.priority = that.priority;
		this.iteratorClass = that.iteratorClass;
		this.name = that.iteratorClass;
		this.options = (that.options != null) ? Sets.newHashSet(that.options) : null;
	}

}
