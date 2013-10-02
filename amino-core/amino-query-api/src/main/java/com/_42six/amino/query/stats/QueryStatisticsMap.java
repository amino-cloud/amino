package com._42six.amino.query.stats;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Simple class for doing benchmarking of the queries
 */
public class QueryStatisticsMap {
	
	@SuppressWarnings("serial")
	private static final ArrayList<TimeRange> rangeList = new ArrayList<TimeRange>() {{
		add(new TimeRange(0, 15));
		add(new TimeRange(15, 30));
		add(new TimeRange(30, 60));
		add(new TimeRange(60, 120));
		add(new TimeRange(120, 240));
	}};
	
	@SuppressWarnings("serial")
	private final HashMap<TimeRange, Integer> bucketTimeMap = new HashMap<TimeRange, Integer>() {{
		for (TimeRange tr : rangeList) {
			put(tr, 0);
		}
	}};

	private long startTime = 0;
	private long endTime = 0;
	private int over = 0;
	private int count = 0;
	
	public QueryStatisticsMap() {
		startTime = System.currentTimeMillis();
	}
	
	public void increment() {
		++count;
		long elapsed = (System.currentTimeMillis() - startTime) / 1000;
		for (TimeRange tr : rangeList) {
			if (elapsed >= tr.start && elapsed < tr.end) {
				bucketTimeMap.put(tr, bucketTimeMap.get(tr) + 1);
				return;
			}
		}
		++over;
	}
	
	public void endTime() {
		endTime = System.currentTimeMillis();
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (TimeRange tr : rangeList) {
			Integer i = bucketTimeMap.get(tr);
			if (i > 0) {
				sb.append(tr.start);
				sb.append("-");
				sb.append(tr.end);
				sb.append(":");
				sb.append(i);
				sb.append("|");
			}
		}
		if (over != 0) {
			sb.append("over:");
			sb.append(over);
			sb.append("|");
		}
		sb.append("elapsed:");
		sb.append(Math.round((endTime - startTime)/1000));
		sb.append("|");
		sb.append("count:");
		sb.append(count);
		return sb.toString();
	}
	
	private static class TimeRange {
		public final int start;
		public final int end;
		
		public TimeRange(int start, int end) {
			this.start = start;
			this.end = end;
		}
	}
}
