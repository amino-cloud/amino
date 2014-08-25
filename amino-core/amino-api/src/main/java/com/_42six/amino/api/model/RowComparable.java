package com._42six.amino.api.model;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RowComparable extends Row implements Comparable<RowComparable> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6267845655335385018L;
	
	private String comparable = null;
	
	public RowComparable(MapWritable mw, Text sortFieldKey) {
		super(mw);
		for (Writable writable : mw.keySet()) {
			put(writable.toString(), mw.get(writable).toString());
		}
		if (mw.get(sortFieldKey) != null) {
			this.comparable = mw.get(sortFieldKey).toString();
		}
	}
	
	@Override
	public int compareTo(RowComparable row) {
		// This logic ensures that when sorting, null sort fields show up at the beginning
		if (comparable.equals(row.comparable)) {
			return 0;
		}
		else if (row.comparable == null) {
			return 1;
		}
		else if (comparable == null) {
			return -1;
		}
		return comparable.compareTo(row.comparable);
	}
}
