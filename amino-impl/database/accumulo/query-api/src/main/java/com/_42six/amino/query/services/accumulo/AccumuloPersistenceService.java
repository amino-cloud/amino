package com._42six.amino.query.services.accumulo;

import com._42six.amino.common.MorePreconditions;
import com.google.common.base.Preconditions;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Abstraction of common operations involving interaction with Accumulo such as
 * Scanning and Writing
 */
public class AccumuloPersistenceService {

    /**
     * Creates the service for interacting with the Cloudbase instance
     * @param connector The Connector for interacting with the database
     */
    public AccumuloPersistenceService(Connector connector){
        this.connector = connector;
    }

    /**
     * Creates the service for interacting with the Accumulo instance
     * @param instanceName The instance to connect to
     * @param zooKeepers The zookeeper to connect to
     * @param accumuloUser The user for Accumulo
     * @param accumuloPassword The password for Accumulo
     * @param useMock Set to true if you want to use a mock instance of Accumulo instead of connection to a real instance
     * @throws IOException if there are problems trying to make the connection
     */
	public AccumuloPersistenceService(String instanceName, String zooKeepers, String accumuloUser, String accumuloPassword, boolean useMock) throws IOException {
        Instance dbInstance;
        if (useMock) {
            dbInstance = new MockInstance(instanceName);
        } else {
            dbInstance = new ZooKeeperInstance(instanceName, zooKeepers);
        }
        try {
            this.connector = dbInstance.getConnector(accumuloUser, accumuloPassword);
        } catch (AccumuloException e) {
            throw new IOException(e);
        } catch (AccumuloSecurityException e) {
            throw new IOException(e);
        }
    }

    /**
     * Creates the service for interactings with the Accumulo instance
     * @param instanceName The instance to connect to
     * @param zooKeepers The zookeeper to connect to
     * @param accumuloUser The user for Accumulo
     * @param accumuloPassword The password for Accumulo
     * @throws IOException if there are problems trying to make the connection
     */
	public AccumuloPersistenceService(String instanceName, String zooKeepers, String accumuloUser, String accumuloPassword) throws IOException {
		this(instanceName, zooKeepers, accumuloUser, accumuloPassword, false);
	}

    /***
     * @return the Connector used to connect to Accumulo
     */
	public Connector getConnector() {
		return this.connector;
	}

	/**
	 * Create a table with the table name passed
	 *
	 * @param tableName The name of the table to create
	 */
	public void createTable(String tableName) throws AccumuloSecurityException, AccumuloException, TableExistsException {
		this.connector.tableOperations().create(tableName);
	}

	/**
	 * Returns true if a table exists with the name passed, or
	 * false otherwise.
	 *
	 * @param tableName The name of the table to check
	 * @return true if table exists false otherwise
	 */
	public Boolean tableExists(String tableName) {
		return this.connector.tableOperations().exists(tableName);
	}

	/**
	 * Insert a row into Accumulo with values provided
	 *
	 * @param rowId The row of the row to insert
	 * @param columnFamily The cf of the row to insert
	 * @param columnQualifier The cq of the row to insert
	 * @param visibility The BT visibility of the row to insert
	 * @param value The value of the row to insert
	 * @param tableName The table to insert the row into
	 */
	public void insertRow(String rowId, String columnFamily, String columnQualifier, String visibility, String value, String tableName) throws Exception {
		final Mutation row = createInsertMutation(rowId, columnFamily, columnQualifier, visibility, value);
		writeCellMutations(new ArrayList<Mutation>(Arrays.asList(row)), tableName);
	}

	/**
	 * Inserts the rows passed. Rows are in the form of a list of maps
	 * with the following keys:
	 * rowid,columnFamily,columnQualifier,visibility,value
	 *
	 * @param rows The rows to insert
	 * @param tableName The table to insert the row into
	 */
	public void insertRows(Collection<Mutation> rows, String tableName) throws Exception {
		writeCellMutations(rows, tableName);
	}


	/**
	 * Create and write a collection of mutations to accumulo using a batch writer
	 *
	 * @param rows     The BtMetadata to persist or delete
	 * @param tableName    The table where the mutations will be writter
	 */
	private void writeCellMutations(Iterable<Mutation> rows, String tableName) throws Exception {
		Preconditions.checkNotNull(rows);
		MorePreconditions.checkNotNullOrEmpty(tableName);

		BatchWriter writer = null;
		try{
	        writer = createBatchWriter(tableName);
			writer.addMutations(rows);
		} finally {
			if(writer != null){
				writer.close();
			}
		}
	}

	/**
	 * Create Mutation for inserts
	 *
	 * @param rowId           The rowid for the cell.
	 * @param columnFamily    The column family for the cell.
	 * @param columnQualifier The column qualifier for the cell.
	 * @param visibility      The visibility for the row
	 * @param value           The value 
	 */
	public Mutation createInsertMutation(String rowId, String columnFamily, String columnQualifier, String visibility, String value) {
		final Mutation m = new Mutation(new Text(rowId));
		m.put(new Text(columnFamily), new Text(columnQualifier), new ColumnVisibility(visibility), new Value(value.getBytes()));
		return m;
	}

	/**
	 * Create Mutation for deletes
	 *
	 * @param rowId           The rowid for the cell.
	 * @param columnFamily    The column family for the cell.
	 * @param columnQualifier The column qualifier for the cell.
	 * @param visibility      The visibility for the row
	 */
	public Mutation createDeleteMutation(String rowId, String columnFamily, String columnQualifier, String visibility) {
        final Mutation m = new Mutation(new Text(rowId));
        m.putDelete(new Text(columnFamily), new Text(columnQualifier), new ColumnVisibility(visibility));
		return m;
    }

    /**
     * Creates a BatchWriter for the given table.  Remember to close() when done
     * @param tableName the table to write to
     * @return BatchWriter for writing to said table
     * @throws TableNotFoundException if the table could not be found
     */
	public BatchWriter createBatchWriter(String tableName) throws TableNotFoundException {
		return this.connector.createBatchWriter(tableName, BATCHWRITER_MAXMEMORY, BATCHWRITER_MAXLATENCY, BATCHWRITER_MAXWRITETHREADS);
	}

    /**
     * Creates a Scanner for reading values from the table
     * @param tableName the table to retrieve values from
     * @param auths the Accumulo Authorizations for retrieving the values
     * @return a Scanner for reading values from the table
     * @throws TableNotFoundException if the table could not be found
     */
	public Scanner createScanner(String tableName, Authorizations auths) throws TableNotFoundException {
		log.debug("Creating a Scanner for " + tableName);
		return this.connector.createScanner(tableName, auths != null ? auths : Constants.NO_AUTHS);
	}

    /**
     * Creates a BatchScanner for reading ranges of values from the table.  Don't forget to close() when done
     * @param tableName the table to retrieve values from
     * @param auths the Accumulo Authorizations for retrieving the values
     * @return a BatchScanner for reading values from the table.  [close() when done]
     * @throws TableNotFoundException if the table could not be found
     */
	public BatchScanner createBatchScanner(String tableName, Authorizations auths) throws TableNotFoundException {
		log.debug("Creating a BatchScanner for " + tableName);
		return this.connector.createBatchScanner(tableName, auths != null ? auths : Constants.NO_AUTHS, BATCHSCANNER_NUMQUERYTHREADS);
	}

    /**
     * Creates a BatchDeleter for deleting values from the table
     * @param tableName the table to delete values from
     * @param auths the Accumulo Authorizations for deleting the values
     * @return a BatchDeleter for deleting values from the table
     * @throws TableNotFoundException if the table could not be found
     */
	public BatchDeleter createBatchDeleter(String tableName, Authorizations auths) throws TableNotFoundException {
		log.debug("Creating a BatchDeleter for " + tableName);
		return this.connector.createBatchDeleter(tableName, auths != null ? auths : Constants.NO_AUTHS, BATCHSCANNER_NUMQUERYTHREADS, BATCHWRITER_MAXMEMORY, BATCHWRITER_MAXLATENCY, BATCHWRITER_MAXWRITETHREADS);
	}

	public Authorizations getLoggedInUserAuthorizations() throws AccumuloException, AccumuloSecurityException {
		return getSecurityOperations().getUserAuthorizations(this.connector.whoami());
	}

	private SecurityOperations getSecurityOperations() {
		return this.connector.securityOperations();
	}

    /**
     * Creates and configures a BatchScanner for reading values from the table
     * @param tableName the table to retrieve values from
     * @param auths the Accumulo Authorizations for retrieving the values
     * @param config how you'd like the BatchScanner to be configured
     * @return a Scanner for reading values from the table
     * @throws TableNotFoundException if the table could not be found
     */
	public BatchScanner createConfiguredBatchScanner(String tableName, Authorizations auths, AccumuloScanConfig config) throws TableNotFoundException, IOException {
		BatchScanner scan = createBatchScanner(tableName, auths);
		configureBatchScanner(scan, config);
		return scan;
	}

    /**
     * Configures an existing BatchScanner for finding particular values
     * @param scanner The scanner to configure
     * @param config The parameters for how you want it configured
     * @throws IOException
     */
	public void configureBatchScanner(BatchScanner scanner, AccumuloScanConfig config) throws IOException {
		scanner.setRanges(generateRanges(config));
		configureBaseScanner(scanner, config);
	}

    /**
     * Configures a BatchDeleter to deletee
     * @param deleter The BatchDeleter to configure
     * @param config The configuration values for what to delete
     * @throws IOException
     */
	public void configureBatchDeleter(BatchDeleter deleter, AccumuloScanConfig config) throws  IOException {
		deleter.setRanges(generateRanges(config));
		configureBaseScanner(deleter, config);
	}

	private void configureBaseScanner(ScannerBase scan, AccumuloScanConfig config) throws IOException {
		if (config.getCustomIterator() != null){
			setCustomIteratorOnScanner(scan, config.getCustomIterator());
		}

		if (config.getColumnFamily() != null) {
			if (config.getColumnQualifier() != null) {
				scan.fetchColumn(new Text(config.getColumnFamily()), new Text(config.getColumnQualifier()));
			} else {
				scan.fetchColumnFamily(new Text(config.getColumnFamily()));
			}
		}

		if (config.getColumnQualifierRegex() != null) {
			scan.setColumnQualifierRegex(config.getColumnQualifierRegex());
		}

	}

    /**
     * Configures an iterator for the ScannerBase
     * @param scan The ScannerBase to set the scanIterator on
     * @param iteratorConfig The configuration of the scanIterator
     * @throws IOException
     */
	public void setCustomIteratorOnScanner(ScannerBase scan, final ScanIteratorConfig iteratorConfig) throws IOException {
		scan.setScanIterators(iteratorConfig.priority, iteratorConfig.iteratorClass, iteratorConfig.name);
		for(ScanIteratorOption option : iteratorConfig.options){
			scan.setScanIteratorOption(iteratorConfig.name, option.getKey(), option.getValue());
		}
	}

	public List<Range> generateRanges(AccumuloScanConfig config) {
		if (isFullTableScan(config)) {
			return new ArrayList<Range>(Arrays.asList(new Range()));
		}

		final ArrayList<Range> ranges = new ArrayList<Range>();

		if(config.getRanges() != null){
			ranges.addAll(config.getRanges());
		}

		ranges.addAll(createShardCountRanges(config));
		if (ranges.size() == 0) {
			ranges.add(createRangeForConfig(config));
		}

		return ranges;
	}

	private boolean isFullTableScan(AccumuloScanConfig config) {
        return config == null ||
               !(config.getRow() != null || config.getStartRow() != null || config.getEndRow() != null || config.getRanges() != null);
	}

	private Range createRangeForConfig(AccumuloScanConfig config, String prefix) {
        AccumuloScanConfig clonedConfig = new AccumuloScanConfig(config);

		if (config.getRow() != null){
			return createRowRange(config, prefix);
		}

		clonedConfig.setStartRow((prefix != null ? prefix : EMPTY_STRING) + ( config.getStartRow() != null ? config.getStartRow() : EMPTY_STRING));

		if (config.getEndRow() != null){
			clonedConfig.setEndRow((prefix != null ? prefix : EMPTY_STRING) + config.getEndRow());
		}

		final Range range;

		Key startKey = new Key(clonedConfig.getStartRow(),
				(clonedConfig.getStartColumnFamily() != null) ? clonedConfig.getStartColumnFamily() : EMPTY_STRING,
				(clonedConfig.getStartColumnQualifier() != null) ? clonedConfig.getStartColumnQualifier() : EMPTY_STRING);

		Key endKey = new Key(clonedConfig.getEndRow(),
				(clonedConfig.getStartColumnFamily() != null) ? clonedConfig.getEndColumnFamily() : EMPTY_STRING,
				(clonedConfig.getStartColumnQualifier() != null) ? clonedConfig.getEndColumnQualifier() : EMPTY_STRING);

		if (clonedConfig.isEndInclusive()) {
			range = new Range(startKey, clonedConfig.isStartInclusive(), endKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false);
		} else {
			range = new Range(startKey, endKey);
		}

		return range;
	}

    /**
     * Creates a Range based on the configuration params
     * @param config paramters for creating the Range
     * @return a Range representing the configuration values
     */
	public Range createRangeForConfig(AccumuloScanConfig config) {
		return createRangeForConfig(config, null);
	}


    /**
     * Creates a Range for 0..shardcount based on the config
     * @param config the parameters of what you're looking for
     * @return Ranges covering all of the shards
     */
	public List<Range> createShardCountRanges(final AccumuloScanConfig config) {
		List<Range> ranges = new ArrayList<Range>();
		if (config.getShardcount() != null) {
			for(int i = 0; i < config.getShardcount(); i++){
				ranges.add(createRangeForConfig(config, i + ":"));
			}
		}
		return ranges;
	}

	private Range createRowRange(AccumuloScanConfig config, String prefix) {
		return new Range((prefix != null ? prefix : EMPTY_STRING) + config.getRow());
	}

    /***********************
     * Fields and constants
     **********************/

	private Connector connector;

	public static final Logger log = Logger.getLogger(AccumuloPersistenceService.class);

	private static final String EMPTY_STRING = "";
	private static final Long BATCHWRITER_MAXMEMORY = 1000000L;
	private static final Long BATCHWRITER_MAXLATENCY = 1000L;
	private static final int BATCHWRITER_MAXWRITETHREADS = 10;
	private static final Integer BATCHSCANNER_NUMQUERYTHREADS = 15;
}
