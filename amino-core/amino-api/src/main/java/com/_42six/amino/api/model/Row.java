package com._42six.amino.api.model;

import java.util.HashMap;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class Row extends HashMap<String, String> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4724989377131204234L;

	public Row() {
		super();
	}
	
	public Row(MapWritable mw) {
		super();
		for (Writable writable : mw.keySet()) {
			put(writable.toString(), mw.get(writable).toString());
		}
	}

}
