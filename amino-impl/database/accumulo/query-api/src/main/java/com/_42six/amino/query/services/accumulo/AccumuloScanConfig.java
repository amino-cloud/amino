package com._42six.amino.query.services.accumulo;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;

import java.util.List;

/**
 * Object to store parameters for the configuration of Accumulo scanners
 */
public class AccumuloScanConfig {

    /** The rowId of the Key to fetch */
	public String row;

    /** When scanning for multiple values, this is the rowId of the Key to start with */
	public String startRow;

    /** When scanning for multiple values, this is the rowId of the Key to end with */
	public String endRow;

    /** If you already know specifically what Ranges you want to scan, use this */
	public List<Range> ranges;

    /** Whether the start row should be included */
	public boolean startInclusive;

    /** Whether the end row is inclusive */
	public boolean endInclusive;

    /** The ColumnFamily to search in */
	public String columnFamily;

    /** The ColumnQualifier to search in */
	public String columnQualifier;

    /** The first ColumnFamily value when searching over multiple ColumnFamilies*/
	public String startColumnFamily;

    /** The first ColumnQualifier value when searching over multiple ColumnQualifiers*/
	public String startColumnQualifier;

    /** The last ColumnFamily value when searching over multiple ColumnFamilies*/
	public String endColumnFamily;

    /** The last ColumnQualifier value when searching over multiple ColumnQualifier*/
	public String endColumnQualifier;

    /** A Regular expression for choosing which ColumnQualifiers to search */
	public String columnQualifierRegex;

    /** The configuration for any custom iterators for the scanner */
	public IteratorSetting iteratorSetting;

    /** The number of shards in the database */
	public Integer shardcount;


	/**
	 * Empty constructor
	 */
	public AccumuloScanConfig() {
		// EMPTY
	}

	/**
	 * Copy constructor
	 * @param that The BtScanConfig to copy
	 */
	public AccumuloScanConfig(AccumuloScanConfig that){
		this.setRow(that.getRow());
		this.setStartRow(that.getStartRow());
		this.setEndRow(that.getEndRow());
		this.setRanges((that.getRanges() != null) ? Lists.newArrayList(that.getRanges()) : null);
		this.setStartInclusive(that.isStartInclusive());
		this.setEndInclusive(that.isEndInclusive());
		this.setColumnFamily(that.getColumnFamily());
		this.setColumnQualifier(that.getColumnQualifier());
		this.setStartColumnFamily(that.getStartColumnFamily());
		this.setStartColumnQualifier(that.getStartColumnQualifier());
		this.setEndColumnFamily(that.getEndColumnFamily());
		this.setEndColumnQualifier(that.getEndColumnQualifier());
		this.setColumnQualifierRegex(that.getColumnQualifierRegex());
        this.setIteratorSetting(that.iteratorSetting);
        this.setShardcount(that.getShardcount());
	}

    public String getRow() {
        return row;
    }

    public AccumuloScanConfig setRow(String row) {
        this.row = row;
        return this;
    }

    public String getStartRow() {
        return startRow;
    }

    public AccumuloScanConfig setStartRow(String startRow) {
        this.startRow = startRow;
        return this;
    }

    public String getEndRow() {
        return endRow;
    }

    public AccumuloScanConfig setEndRow(String endRow) {
        this.endRow = endRow;
        return this;
    }

    public List<Range> getRanges() {
        return ranges;
    }

    public AccumuloScanConfig setRanges(List<Range> ranges) {
        this.ranges = ranges;
        return this;
    }

    public boolean isStartInclusive() {
        return startInclusive;
    }

    public AccumuloScanConfig setStartInclusive(boolean startInclusive) {
        this.startInclusive = startInclusive;
        return this;
    }

    public boolean isEndInclusive() {
        return endInclusive;
    }

    public AccumuloScanConfig setEndInclusive(boolean endInclusive) {
        this.endInclusive = endInclusive;
        return this;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public AccumuloScanConfig setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
        return this;
    }

    public String getColumnQualifier() {
        return columnQualifier;
    }

    public AccumuloScanConfig setColumnQualifier(String columnQualifier) {
        this.columnQualifier = columnQualifier;
        return this;
    }

    public String getStartColumnFamily() {
        return startColumnFamily;
    }

    public AccumuloScanConfig setStartColumnFamily(String startColumnFamily) {
        this.startColumnFamily = startColumnFamily;
        return this;
    }

    public String getStartColumnQualifier() {
        return startColumnQualifier;
    }

    public AccumuloScanConfig setStartColumnQualifier(String startColumnQualifier) {
        this.startColumnQualifier = startColumnQualifier;
        return this;
    }

    public String getEndColumnFamily() {
        return endColumnFamily;
    }

    public AccumuloScanConfig setEndColumnFamily(String endColumnFamily) {
        this.endColumnFamily = endColumnFamily;
        return this;
    }

    public String getEndColumnQualifier() {
        return endColumnQualifier;
    }

    public AccumuloScanConfig setEndColumnQualifier(String endColumnQualifier) {
        this.endColumnQualifier = endColumnQualifier;
        return this;
    }

    public String getColumnQualifierRegex() {
        return columnQualifierRegex;
    }

    public AccumuloScanConfig setColumnQualifierRegex(String columnQualifierRegex) {
        this.columnQualifierRegex = columnQualifierRegex;
        return this;
    }

    public IteratorSetting getIteratorSetting() {
        return iteratorSetting;
    }

    public AccumuloScanConfig setIteratorSetting(IteratorSetting customIterator) {
        this.iteratorSetting = customIterator;
        return this;
    }

    public Integer getShardcount() {
        return shardcount;
    }

    public AccumuloScanConfig setShardcount(Integer shardcount) {
        this.shardcount = shardcount;
        return this;
    }
}
