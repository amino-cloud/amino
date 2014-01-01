package com._42six.amino.query.services;


import com._42six.amino.query.exception.BigTableException;

import java.util.Set;

public interface AminoPersistenceService {
    /**
     * Create a table with the table name passed
     *
     * @param tableName The name of the table to create
     */
    void createTable(String tableName) throws BigTableException;

    /**
     * Returns true if a table exists with the name passed, or
     * false otherwise.
     *
     * @param tableName The name of the table to check
     * @return true if table exists false otherwise
     */
    Boolean tableExists(String tableName);

    /**
     * Insert a row into the BigTable database with values provided
     *
     * @param rowId The row of the row to insert
     * @param columnFamily The cf of the row to insert
     * @param columnQualifier The cq of the row to insert
     * @param visibility The BT visibility of the row to insert
     * @param value The value of the row to insert
     * @param tableName The table to insert the row into
     */
    void insertRow(String rowId, String columnFamily, String columnQualifier, String visibility, String value, String tableName) throws BigTableException;

    Set<String> getLoggedInUserAuthorizations() throws BigTableException;
}
