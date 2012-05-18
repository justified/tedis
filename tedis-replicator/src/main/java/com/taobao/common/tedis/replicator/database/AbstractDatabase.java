/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.data.OneRowChange;

public abstract class AbstractDatabase implements Database {
    private static Logger logger = Logger.getLogger(AbstractDatabase.class);

    protected String dbDriver = null;
    protected String dbUri = null;
    protected String dbUser = null;
    protected String dbPassword = null;
    protected Connection dbConn = null;
    protected boolean autoCommit = false;
    protected String defaultSchema = null;

    protected static boolean driverLoaded = false;
    protected boolean connected = false;

    public AbstractDatabase() {
    }

    public Connection getConnection() {
        return dbConn;
    }

    public boolean connected() {
        return connected;
    }

    public abstract SqlOperationMatcher getSqlNameMatcher() throws ReplicatorException;

    public void setUrl(String dbUri) {
        this.dbUri = dbUri;
    }

    public void setUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public void setPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public String getPlaceHolder(OneRowChange.ColumnSpec col, Object colValue, String typeDesc) {
        return " ? ";
    }

    public boolean nullsBoundDifferently(OneRowChange.ColumnSpec col) {
        return false;
    }

    public boolean nullsEverBoundDifferently() {
        return false;
    }

    abstract protected String columnToTypeString(Column c);

    public synchronized void connect() throws SQLException {
        connect(false);
    }

    public synchronized void connect(boolean binlog) throws SQLException {
        if (dbConn == null) {
            if (!driverLoaded && dbDriver != null) {
                try {
                    logger.info("Loading database driver: " + dbDriver);
                    Class.forName(dbDriver);
                    driverLoaded = true;
                } catch (Exception e) {
                    throw new RuntimeException("Unable to load driver: " + dbDriver, e);
                }
            }

            dbConn = DriverManager.getConnection(dbUri, dbUser, dbPassword);
            connected = (dbConn != null);
        }
    }

    public synchronized void disconnect() {
        if (dbConn != null) {
            try {
                dbConn.close();
            } catch (SQLException e) {
                logger.warn("Unable to close connection", e);
            }
            dbConn = null;
            connected = false;
        }
    }

    public DatabaseMetaData getDatabaseMetaData() throws SQLException {
        return dbConn.getMetaData();
    }

    public boolean supportsCreateDropSchema() {
        return false;
    }

    public void createSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException("Creating schema is not supported");
    }

    public void dropSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException("Dropping schema is not supported");
    }

    public boolean supportsUseDefaultSchema() {
        return false;
    }

    public void useDefaultSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException("Setting the default schema is not supported");
    }

    public String getUseSchemaQuery(String schema) {
        throw new UnsupportedOperationException("Getting the default schema is not supported");
    }

    public boolean supportsControlSessionLevelLogging() {
        return false;
    }

    public void controlSessionLevelLogging(boolean suppressed) throws SQLException {
        throw new UnsupportedOperationException("Controlling session level logging is not supported");
    }

    public boolean supportsNativeSlaveSync() {
        return false;
    }

    public void syncNativeSlave(String eventId) throws SQLException {
        throw new UnsupportedOperationException("Native slave synchronization is not supported");
    }

    public boolean supportsControlTimestamp() {
        return false;
    }

    public String getControlTimestampQuery(Long timestamp) {
        throw new UnsupportedOperationException("Controlling session level logging is not supported");
    }

    public boolean supportsSessionVariables() {
        return false;
    }

    public void setSessionVariable(String name, String value) throws SQLException {
        throw new UnsupportedOperationException("Session variables are not supported");
    }

    public String getSessionVariable(String name) throws SQLException {
        throw new UnsupportedOperationException("Session variables are not supported");
    }

    public void execute(String SQL) throws SQLException {
        Statement sqlStatement = null;
        try {
            sqlStatement = dbConn.createStatement();
            if (logger.isDebugEnabled())
                logger.debug(SQL);
            sqlStatement.execute(SQL);
        } finally {
            if (sqlStatement != null)
                sqlStatement.close();
        }
    }

    public void executeUpdate(String SQL) throws SQLException {
        Statement sqlStatement = null;

        try {
            sqlStatement = dbConn.createStatement();
            if (logger.isDebugEnabled())
                logger.debug(SQL);
            sqlStatement.executeUpdate(SQL);
        } finally {
            sqlStatement.close();
        }
    }

    private String buildWhereClause(ArrayList<Column> columns) {
        if (columns.size() == 0)
            return "";

        StringBuffer retval = new StringBuffer(" WHERE ");

        Iterator<Column> i = columns.iterator();
        boolean comma = false;
        while (i.hasNext()) {
            Column c = i.next();
            if (comma)
                retval.append(" AND ");
            comma = true;
            retval.append(assignString(c));
        }
        return retval.toString();
    }

    private String buildCommaAssign(ArrayList<Column> columns) {
        StringBuffer retval = new StringBuffer();
        Iterator<Column> i = columns.iterator();
        boolean comma = false;
        while (i.hasNext()) {
            Column c = i.next();
            if (comma)
                retval.append(", ");
            comma = true;
            retval.append(assignString(c));
        }
        return retval.toString();
    }

    private String buildCommaValues(ArrayList<Column> columns) {
        StringBuffer retval = new StringBuffer();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0)
                retval.append(", ");
            retval.append("?");
        }
        return retval.toString();
    }

    private String assignString(Column c) {
        return c.getName() + "= ?";
    }

    private int executePrepareStatement(List<Column> columns, PreparedStatement statement) throws SQLException {
        int bindNo = 1;

        for (Column c : columns) {
            statement.setObject(bindNo++, c.getValue());
        }
        // System.out.format("%s (%d binds)\n", SQL, bindNo - 1);
        return statement.executeUpdate();
    }

    private int executePrepareStatement(Table table, PreparedStatement statement) throws SQLException {
        return executePrepareStatement(table.getAllColumns(), statement);
    }

    private int executePrepare(Table table, List<Column> columns, String SQL) throws SQLException {
        return executePrepare(table, columns, SQL, false, -1);
    }

    private int executePrepare(Table table, String SQL) throws SQLException {
        return executePrepare(table, table.getAllColumns(), SQL, false, -1);
    }

    private int executePrepare(Table table, List<Column> columns, String SQL, boolean keep, int type) throws SQLException {
        int bindNo = 1;

        PreparedStatement statement = null;
        int affectedRows = 0;

        try {
            statement = dbConn.prepareStatement(SQL);

            for (Column c : columns) {
                statement.setObject(bindNo++, c.getValue());
            }
            affectedRows = statement.executeUpdate();
        } finally {
            if (statement != null && !keep) {
                statement.close();
                statement = null;
            }
        }
        if (keep && type > -1)
            table.setStatement(type, statement);

        return affectedRows;
    }

    public int insert(Table table) throws SQLException {
        String SQL = "";
        PreparedStatement statement = null;
        boolean caching = table.getCacheStatements();
        ArrayList<Column> allColumns = table.getAllColumns();

        if (caching && (statement = table.getStatement(Table.INSERT)) != null) {
            return executePrepareStatement(table, statement);
        } else {
            SQL += "INSERT INTO " + table.getSchema() + "." + table.getName() + " VALUES (";
            SQL += buildCommaValues(allColumns);
            SQL += ")";
        }

        return executePrepare(table, allColumns, SQL, caching, Table.INSERT);
    }

    public int update(Table table, ArrayList<Column> whereClause, ArrayList<Column> values) throws SQLException {
        StringBuffer sb = new StringBuffer("UPDATE ");
        sb.append(table.getSchema());
        sb.append(".");
        sb.append(table.getName());
        sb.append(" SET ");
        sb.append(buildCommaAssign(values));
        if (whereClause != null) {
            sb.append(" ");
            sb.append(buildWhereClause(whereClause));
        }
        String SQL = sb.toString();

        ArrayList<Column> allColumns = new ArrayList<Column>(values);
        if (whereClause != null) {
            allColumns.addAll(whereClause);
        }
        return this.executePrepare(table, allColumns, SQL);
    }

    public boolean supportsReplace() {
        return false;
    }

    public void replace(Table table) throws SQLException {
        if (supportsReplace()) {
            String SQL = "";
            SQL += "REPLACE INTO " + table.getSchema() + "." + table.getName() + " VALUES (";
            SQL += buildCommaValues(table.getAllColumns());
            SQL += ")";

            executePrepare(table, SQL);
        } else {
            try {
                delete(table, false);
            } catch (SQLException e) {
            }
            insert(table);
        }
    }

    public int delete(Table table, boolean allRows) throws SQLException {
        String SQL = "DELETE FROM " + table.getSchema() + "." + table.getName() + " ";
        if (!allRows) {
            SQL += buildWhereClause(table.getPrimaryKey().getColumns());
            return executePrepare(table, table.getPrimaryKey().getColumns(), SQL);
        } else
            return executePrepare(table, new ArrayList<Column>(), SQL);
    }

    public PreparedStatement prepareStatement(String statement) throws SQLException {
        if (logger.isDebugEnabled())
            logger.debug("prepareStatement" + statement);
        return dbConn.prepareStatement(statement);
    }

    public Statement createStatement() throws SQLException {
        if (logger.isDebugEnabled())
            logger.debug("createStatement");
        return dbConn.createStatement();
    }

    public void commit() throws SQLException {
        if (logger.isDebugEnabled())
            logger.debug("commit");
        dbConn.commit();
    }

    public void rollback() throws SQLException {
        if (logger.isDebugEnabled())
            logger.debug("rollback");
        dbConn.rollback();
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
        if (logger.isDebugEnabled())
            logger.debug("setAutoCommit = " + autoCommit);
        if (dbConn.getAutoCommit() != autoCommit)
            dbConn.setAutoCommit(autoCommit);
    }

    public void createTable(Table t, boolean replace) throws SQLException {
        boolean comma = false;

        if (replace)
            dropTable(t);

        String SQL = "CREATE TABLE " + t.getSchema() + "." + t.getName() + " (";

        Iterator<Column> i = t.getAllColumns().iterator();
        while (i.hasNext()) {
            Column c = i.next();
            SQL += (comma ? ", " : "") + c.getName() + " " + columnToTypeString(c) + (c.isNotNull() ? " NOT NULL" : "");
            comma = true;
        }
        Iterator<Key> j = t.getKeys().iterator();

        while (j.hasNext()) {
            Key key = j.next();
            SQL += ", ";
            switch (key.getType()) {
            case Key.Primary:
                SQL += "PRIMARY KEY (";
                break;
            case Key.Unique:
                SQL += "UNIQUE (";
                break;
            case Key.NonUnique:
                SQL += "KEY (";
                break;
            }
            i = key.getColumns().iterator();
            comma = false;
            while (i.hasNext()) {
                Column c = i.next();
                SQL += (comma ? ", " : "") + c.getName();
                comma = true;
            }
            SQL += ")";
        }
        SQL += ")";

        // Create the table.
        execute(SQL);
    }

    public void dropTable(Table table) {
        String SQL = "DROP TABLE " + table.getSchema() + "." + table.getName() + " ";

        try {
            execute(SQL);
        } catch (SQLException e) {
            if (logger.isDebugEnabled())
                logger.debug("Unable to drop table; this may be expected", e);
        }
    }

    public void close() {
        disconnect();
    }

    public int nativeTypeToJavaSQLType(int nativeType) throws SQLException {
        return nativeType;
    }

    public int javaSQLTypeToNativeType(int javaSQLType) throws SQLException {
        return javaSQLType;
    }

    public Table findTable(int tableID) throws SQLException {
        return null;
    }

    public Table findTable(int tableID, String scn) throws SQLException {
        return null;
    }

    public abstract ResultSet getColumnsResultSet(DatabaseMetaData md, String schemaName, String tableName) throws SQLException;

    protected abstract ResultSet getPrimaryKeyResultSet(DatabaseMetaData md, String schemaName, String tableName) throws SQLException;

    protected abstract ResultSet getTablesResultSet(DatabaseMetaData md, String schemaName, boolean baseTablesOnly) throws SQLException;

    public Table findTable(String schemaName, String tableName) throws SQLException {
        DatabaseMetaData md = this.getDatabaseMetaData();
        Table table = null;

        ResultSet rsc = getColumnsResultSet(md, schemaName, tableName);
        if (rsc.isBeforeFirst()) {
            // found columns
            Map<String, Column> cm = new HashMap<String, Column>();
            table = new Table(schemaName, tableName);
            while (rsc.next()) {
                String colName = rsc.getString("COLUMN_NAME");
                int colType = rsc.getInt("DATA_TYPE");
                long colLength = rsc.getLong("COLUMN_SIZE");
                boolean isNotNull = rsc.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls;
                String valueString = rsc.getString("COLUMN_DEF");

                Column column = new Column(colName, colType, colLength, isNotNull, valueString);
                column.setPosition(rsc.getInt("ORDINAL_POSITION"));
                column.setTypeDescription(rsc.getString("TYPE_NAME").toUpperCase());
                table.AddColumn(column);
                cm.put(column.getName(), column);
            }

            ResultSet rsk = getPrimaryKeyResultSet(md, schemaName, tableName);
            if (rsk.isBeforeFirst()) {
                // primary key found
                Key pKey = new Key(Key.Primary);
                while (rsk.next()) {
                    String colName = rsk.getString("COLUMN_NAME");
                    Column column = cm.get(colName);
                    pKey.AddColumn(column);
                }
                table.AddKey(pKey);
            }
            rsk.close();
        }
        rsc.close();

        return table;
    }

    public ArrayList<Table> getTables(String schemaName, boolean baseTablesOnly) throws SQLException {
        DatabaseMetaData md = this.getDatabaseMetaData();
        ArrayList<Table> tables = new ArrayList<Table>();

        try {
            ResultSet rst = getTablesResultSet(md, schemaName, baseTablesOnly);
            if (rst.isBeforeFirst()) {
                while (rst.next()) {
                    String tableName = rst.getString("TABLE_NAME");
                    Table table = findTable(schemaName, tableName);
                    if (table != null) {
                        tables.add(table);
                    }
                }
            }
            rst.close();
        } finally {
        }

        return tables;
    }

    public void createTable(Table table, boolean replace, String tedisTableType) throws SQLException {
        createTable(table, replace);
    }

    public String prepareOptionSetStatement(String optionName, String optionValue) {
        return null;
    }

    public byte[] getBlobAsBytes(ResultSet resultSet, int column) throws SQLException {
        Blob blob = resultSet.getBlob(column);
        return blob.getBytes(1L, (int) blob.length());
    }

    public String getDatabaseObjectName(String name) {
        return name;
    }

}
