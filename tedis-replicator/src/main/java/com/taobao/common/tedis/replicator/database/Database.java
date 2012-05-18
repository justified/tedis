/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.data.OneRowChange;

public interface Database {
    /**
     * Returns a SQL name matcher for this database type. You can get a matcher
     * without calling connect() first.
     *
     * @throws ReplicatorException
     */
    public SqlOperationMatcher getSqlNameMatcher() throws ReplicatorException;

    /** Sets the JDBC URL used for database connections. */
    public void setUrl(String url);

    /** Sets the database user. */
    public void setUser(String user);

    /** Sets the database password. */
    public void setPassword(String password);

    /**
     * Connects to the database. You must set the url, user, and password then
     * do this. Connection does not log queries by default.
     */
    public void connect() throws SQLException;

    /**
     * Connects to the database. You must set the url, user, and password then
     * do this.
     *
     * @param binlog
     *            log connection updates.
     * @throws SQLException
     */
    public void connect(boolean binlog) throws SQLException;


    /**
     * @return true if current connection is connected else false.
     */
    public boolean connected();

    /**
     * Disconnects from the database. This is also accomplished by close().
     */
    public void disconnect();

    /**
     * Returns JDBC DatabaseMetadata for the current connection.
     */
    public DatabaseMetaData getDatabaseMetaData() throws SQLException;

    /**
     * Creates a table using the supplied table definition.
     *
     * @param table
     *            Table specification
     * @param replace
     *            If true, replace an existing table
     * @throws SQLException
     */
    public void createTable(Table table, boolean replace) throws SQLException;

    /**
     * Returns true if this implementation supports schema create and drop
     * operations.
     */
    public boolean supportsCreateDropSchema() throws SQLException;

    /**
     * Creates the named schema.
     *
     * @see #supportsCreateDropSchema()
     */
    public void createSchema(String schema) throws SQLException;

    /**
     * Drops the named schema.
     *
     * @see #supportsCreateDropSchema()
     */
    public void dropSchema(String schema) throws SQLException;

    /**
     * Returns true if this implementation supports changing the default schema,
     * for example via a "USE <database>" command.
     */
    public boolean supportsUseDefaultSchema() throws SQLException;

    /**
     * Changes the default schema to the named schema.
     *
     * @see #supportsUseDefaultSchema()
     */
    public void useDefaultSchema(String schema) throws SQLException;

    /**
     * Returns a query that can be used to set the schema.
     *
     * @see #supportsUseDefaultSchema()
     */
    public String getUseSchemaQuery(String schema);

    /**
     * Returns true if this implementation allow clients to turn logging of SQL
     * updates on and off at the session level. (Currently only MySQL supports
     * this feature.)
     */
    public boolean supportsControlSessionLevelLogging() throws SQLException;

    /**
     * Sets session-level logging of updates.
     *
     * @param suppressed
     *            If true, updates are not logged; otherwise logging is turned
     *            on
     */
    public void controlSessionLevelLogging(boolean suppressed) throws SQLException;

    public boolean supportsNativeSlaveSync();

    public void syncNativeSlave(String eventId) throws SQLException;

    /**
     * Returns true if this implementation supports changing the timestamp value
     * used by functions that return the current time.
     */
    public boolean supportsControlTimestamp() throws SQLException;

    /**
     * Returns true if this implementation supports setting session variables.
     */
    public boolean supportsSessionVariables() throws SQLException;

    /**
     * Sets a session variable. This works only if the database type supports
     * session variables.
     *
     * @param name
     *            Name of the variable to set
     * @param value
     *            Value to set
     * @throws SQLException
     *             Thrown if setting variable is unsuccessful
     */
    public void setSessionVariable(String name, String value) throws SQLException;

    /**
     * Get session variable. This works only if the database type supports
     * session variables.
     *
     * @param name
     *            Name of the variable to get
     * @return Value of variable or null if unset
     * @throws SQLException
     *             Thrown if getting variable is unsuccessful
     */
    public String getSessionVariable(String name) throws SQLException;

    /**
     * Return the Table with all its current accompanying Columns that matches
     * tableID. tableID is meant to be some sort of unique "object number"
     * interpreted within the current connection. The exact nature of of what
     * this tableID will likely vary from rdbms to rdbms but in Oracle parlance
     * it is an object number. Returns null if no such table exists.
     */
    public Table findTable(int tableID) throws SQLException;

    /**
     * Return the Table with all its accompanying Columns at the provided scn
     * that matches tableID. tableID is meant to be some sort of unique "object
     * number" interpreted within the current connection. The exact nature of of
     * what this tableID will likely vary from rdbms to rdbms but in Oracle
     * parlance it is an object number. Returns null if no such table exists.
     *
     * @param tableID
     *            the object which is looked for
     * @param scn
     *            when the object is search for
     * @return a Table if matching was found
     * @throws SQLException
     *             if an error occurs
     */
    public Table findTable(int tableID, String scn) throws SQLException;

    /**
     * Return the Table with all its accompanying Columns.
     *
     * @param schemaName
     *            name of schema containing the table
     * @param tableName
     *            name of the table
     * @return a Table if matching was found
     * @throws SQLException
     *             if an error occurs
     */
    public Table findTable(String schemaName, String tableName) throws SQLException;

    /**
     * Returns a query that can be used to set the timestamp.
     *
     * @param timestamp
     *            A Java time value consisting of milliseconds since January 1,
     *            1970 00:00:00 GMT
     * @see #supportsControlTimestamp()
     */
    public String getControlTimestampQuery(Long timestamp);

    /**
     * Executes a SQL request.
     */
    public void execute(String SQL) throws SQLException;

    /**
     * Executes a SQL request containing an update.
     */
    public void executeUpdate(String SQL) throws SQLException;

    /**
     * Inserts a row into a table. Values are taken from the current Column
     * instances stored in the table definition.
     *
     * @param table
     *            Table instance containing column data
     * @return the number of inserted rows
     */
    public int insert(Table table) throws SQLException;

    /**
     * Updates on or more rows in a table.
     *
     * @param table
     *            Table instance to update
     * @param whereClause
     *            List of columns containing where clause values, which are
     *            ANDed
     * @param values
     *            List of columns containing values to set for matching rows
     * @return the number of updated rows
     */
    public int update(Table table, ArrayList<Column> whereClause, ArrayList<Column> values) throws SQLException;

    /**
     * Returns true if the implementation supports a SQL REPLACE command.
     */
    public boolean supportsReplace();

    /**
     * Replaces a row in the table using the data supplied by the Table
     * specification. The Table instance's primary key is used to locate the
     * matching row. This uses a REPLACE command if it is available.
     *
     * @param table
     *            Table definition with column data and primary key
     * @see #supportsReplace()
     */
    public void replace(Table table) throws SQLException;

    /**
     * Deletes a row in a table.
     *
     * @param table
     *            Table specification with primary key; columns for the key must
     *            be defined.
     * @param allRows
     *            flag indicating that all rows from the underlying table should
     *            be deleted
     * @return the number of deleted rows
     * @throws SQLException
     */
    public int delete(Table table, boolean allRows) throws SQLException;

    /**
     * Generate a JDBC prepared statement.
     *
     * @param statement
     *            SQL statement to prepare
     */
    public PreparedStatement prepareStatement(String statement) throws SQLException;

    /**
     * Generate a JDBC statement.
     */
    public Statement createStatement() throws SQLException;

    /**
     * Commit the current transaction.
     */
    public void commit() throws SQLException;

    /**
     * Rollback the current transaction.
     */
    public void rollback() throws SQLException;

    /**
     * Toggles autocommit by calling Connection.setAutocommit().
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException;

    /**
     * Return a place holder in a prepared statement for a column of type
     * ColumnSpec. Typically "?" as is INSERT INTO FOO VALUES(?)
     */
    public String getPlaceHolder(OneRowChange.ColumnSpec col, Object colValue, String typeDesc);

    /**
     * Return TRUE IFF NULL values are bound differently in SQL statement from
     * non null values for the given column type. For example, in Oracle, the
     * datatype XML must look like "XMLTYPE(?)" in most SQL statements, but in
     * the case of a NULL value, it would look simply like "?".
     */
    public boolean nullsBoundDifferently(OneRowChange.ColumnSpec col);

    /**
     * return true IFF nulls are sometimes treated differently in
     * nullsBoundDifferently() as non nulls.
     */
    public boolean nullsEverBoundDifferently();

    /**
     * Returns the database connection
     */
    public Connection getConnection();

    /**
     * Drops an existing table.
     */
    public void dropTable(Table table);

    /**
     * Closes the instance and frees all resources.
     */
    public void close();

    /**
     * Databases have various column types usually identified by some vendor
     * defined integer value. e.g. 2 = Oracle number, 12 = Oracle date. This
     * function converts the vendor specific type numbers into the java.sql.Type
     * numbers we shall use internally.
     */
    public int nativeTypeToJavaSQLType(int nativeType) throws SQLException;

    /**
     * Opposite of the above function. I cannot image why any of the portable
     * code should ever need to call this function. But it seems like a fine
     * place to create a place holder for such a function declaration even if
     * only called from vendor specific code.
     */
    public int javaSQLTypeToNativeType(int javaSQLType) throws SQLException;

    /**
     * Returns a list of schemas available on the server.
     *
     * @return list of available schemas
     * @throws SQLException
     */
    public ArrayList<String> getSchemas() throws SQLException;

    /**
     * Returns a list of tables available in the schema
     *
     * @param schema
     *            Name of the schema
     * @param baseTablesOnly
     *            If true, only return real tables and not catalogs or views
     * @return list of tables in the schema
     * @throws SQLException
     */
    public ArrayList<Table> getTables(String schema, boolean baseTablesOnly) throws SQLException;

    /**
     * Returns a result set containing columns for a specific table.
     *
     * @param md
     *            DatabaseMetaData object
     * @param schemaName
     *            schema name
     * @param tableName
     *            table name
     * @return ResultSet as produced by DatabaseMetaData.getColumns() for a
     *         given schema and table
     * @throws SQLException
     */
    public ResultSet getColumnsResultSet(DatabaseMetaData md, String schemaName, String tableName) throws SQLException;

    /**
     * getNowFunction returns the database-specific way to get current date and
     * time from the database.
     *
     * @return the name of the function to be called at the database level to
     *         get the current date and time
     */
    public String getNowFunction();

    /**
     * getTimeDiff returns the database-specific way of subtracting two "dates"
     * and return the result in seconds complete with space for the two bind
     * variables. E.g. in MySQL it might be "time_to_sec(timediff(?, ?))". If
     * either of the string variables are null, replace with the bind character
     * (e.g. "?") else use the string given. For example getTimeDiff(null,
     * "myTimeCol") -> time_to_sec(timediff(?, myTimeCol))
     */
    public String getTimeDiff(String string1, String string2);

    public void createTable(Table table, boolean replace, String teTableType) throws SQLException;

    /**
     * prepareOptionSetStatement generates the sql statement that is to be used
     * to set an option (or a session variable) at the database connection
     * level.
     *
     * @param optionName
     *            the option to be set
     * @param optionValue
     *            the value to be used
     * @return a string that contains the statement that should be executed, or
     *         null if this does not exist for a database
     */
    public String prepareOptionSetStatement(String optionName, String optionValue);

    /**
     * Fetches the given column as a byte[] array. For MySQL and Oracle this
     * means converting BLOB field into byte[] array, while for PostgreSQL -
     * returning the bytea type field's value.
     *
     * @param resultSet
     *            ResultSet which has this blob column.
     * @param column
     *            Index of the column to fetch value from.
     * @return byte[] array containing underlying BLOB or bytea field value.
     * @throws SQLException
     *             As this method is operating on ResultSet, it might throw a
     *             SQLException.
     */
    public byte[] getBlobAsBytes(ResultSet resultSet, int column) throws SQLException;

    /**
     * Returns the eventually quoted database object name. For example, with
     * mysql, database object names should be backticked (`example`).
     *
     * @param name
     *            unquoted database object name
     * @return eventually quoted database object name
     */
    public String getDatabaseObjectName(String name);
}
