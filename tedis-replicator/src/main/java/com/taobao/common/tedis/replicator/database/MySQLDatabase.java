/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.data.OneRowChange;

public class MySQLDatabase extends AbstractDatabase {

    public MySQLDatabase() throws SQLException {
        // Hard code the driver so it gets loaded correctly.
        dbDriver = "com.mysql.jdbc.Driver";
    }

    protected String columnToTypeString(Column c) {
        switch (c.getType()) {
        case Types.TINYINT:
            return "TINYINT";

        case Types.SMALLINT:
            return "SMALLINT";

        case Types.INTEGER:
            return "INT";

        case Types.BIGINT:
            return "BIGINT";

        case Types.CHAR:
            return "CHAR(" + c.getLength() + ")";

        case Types.VARCHAR:
            return "VARCHAR(" + c.getLength() + ")";

        case Types.DATE:
            return "DATETIME";

        case Types.TIMESTAMP:
            return "TIMESTAMP";

        case Types.CLOB:
            return "LONGTEXT";

        case Types.BLOB:
            return "LONGBLOB";

        default:
            return "UNKNOWN";
        }
    }

    /**
     * Connect to a MySQL database, which includes setting the wait_timeout to a
     * very high value so we don't lose our connection. {@inheritDoc}
     */
    public void connect() throws SQLException {
        connect(false);
    }

    public void createTable(Table t, boolean replace) throws SQLException {
        createTable(t, replace, null);
    }

    public boolean supportsReplace() {
        return true;
    }

    public boolean supportsUseDefaultSchema() {
        return true;
    }

    public void useDefaultSchema(String schema) throws SQLException {
        execute(getUseSchemaQuery(schema));
        this.defaultSchema = schema;
    }

    public String getUseSchemaQuery(String schema) {
        return "USE " + getDatabaseObjectName(schema);
    }

    public boolean supportsCreateDropSchema() {
        return true;
    }

    public void createSchema(String schema) throws SQLException {
        String SQL = "CREATE DATABASE IF NOT EXISTS " + schema;
        execute(SQL);
    }

    public void dropSchema(String schema) throws SQLException {
        String SQL = "DROP DATABASE IF EXISTS " + schema;
        execute(SQL);
    }

    public boolean supportsControlSessionLevelLogging() {
        return true;
    }

    @Override
    public boolean supportsNativeSlaveSync() {
        return true;
    }

    @Override
    public void syncNativeSlave(String eventId) throws SQLException {
        // Parse the event ID, which has the following format:
        // <file>:<offset>[;<session id>]
        int colonIndex = eventId.indexOf(':');
        String binlogFile = eventId.substring(0, colonIndex);

        int semicolonIndex = eventId.indexOf(";");
        int binlogOffset;
        if (semicolonIndex != -1)
            binlogOffset = Integer.valueOf(eventId.substring(colonIndex + 1, semicolonIndex));
        else
            binlogOffset = Integer.valueOf(eventId.substring(colonIndex + 1));

        // Create a CHANGE MASTER TO command.
        String changeMaster = String.format("CHANGE MASTER TO master_log_file = '%s', master_log_pos = %s", binlogFile, binlogOffset);
        executeUpdate(changeMaster);
    }

    public boolean supportsControlTimestamp() {
        return true;
    }

    public String getControlTimestampQuery(Long timestamp) {
        return "SET TIMESTAMP=" + (timestamp / 1000);
    }

    public boolean supportsSessionVariables() {
        return true;
    }

    public void setSessionVariable(String name, String value) throws SQLException {
        String escapedValue = value.replaceAll("'", "\'");
        execute("SET @" + name + "='" + escapedValue + "'");
    }

    public String getSessionVariable(String name) throws SQLException {
        Statement s = null;
        ResultSet rs = null;
        String value = null;
        try {
            s = dbConn.createStatement();
            rs = s.executeQuery("SELECT @" + name);
            while (rs.next()) {
                value = rs.getString(1);
            }
            rs.close();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
            if (s != null) {
                try {
                    s.close();
                } catch (SQLException e) {
                }
            }
        }
        return value;
    }

    public ArrayList<String> getSchemas() throws SQLException {
        ArrayList<String> schemas = new ArrayList<String>();

        try {
            DatabaseMetaData md = this.getDatabaseMetaData();
            ResultSet rs = md.getCatalogs();
            while (rs.next()) {
                schemas.add(rs.getString("TABLE_CAT"));
            }
            rs.close();
        } finally {
        }

        return schemas;
    }

    public ResultSet getColumnsResultSet(DatabaseMetaData md, String schemaName, String tableName) throws SQLException {
        return md.getColumns(schemaName, null, tableName, null);
    }

    protected ResultSet getPrimaryKeyResultSet(DatabaseMetaData md, String schemaName, String tableName) throws SQLException {
        return md.getPrimaryKeys(schemaName, null, tableName);
    }

    protected ResultSet getTablesResultSet(DatabaseMetaData md, String schemaName, boolean baseTablesOnly) throws SQLException {
        String types[] = null;
        if (baseTablesOnly)
            types = new String[] { "TABLE" };

        return md.getTables(schemaName, null, null, types);
    }

    /**
     * getTimeDiff returns the database-specific way of subtracting two "dates"
     * and return the result in seconds complete with space for the two bind
     * variables. E.g. in MySQL it might be "time_to_sec(timediff(?, ?))". If
     * either of the string variables are null, replace with the bind character
     * (e.g. "?") else use the string given. For example getTimeDiff(null,
     * "myTimeCol") -> time_to_sec(timediff(?, myTimeCol))
     */
    public String getTimeDiff(String string1, String string2) {
        String retval = "time_to_sec(timediff(";
        if (string1 == null)
            retval += "?";
        else
            retval += string1;
        retval += ",";
        if (string2 == null)
            retval += "?";
        else
            retval += string2;
        retval += "))";

        return retval;
    }

    public String getNowFunction() {
        return "now()";
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

    @Override
    public String prepareOptionSetStatement(String optionName, String optionValue) {
        return "set @@session." + optionName + "=" + optionValue;
    }

    @Override
    public void createTable(Table t, boolean replace, String tableType) throws SQLException {
        boolean comma = false;
        String SQL;

        if (replace) {
            this.dropTable(t);
        }

        SQL = "CREATE TABLE ";
        SQL += (replace ? "" : "IF NOT EXISTS ");
        SQL += t.getSchema() + "." + t.getName();
        SQL += " (";

        Iterator<Column> i = t.getAllColumns().iterator();
        while (i.hasNext()) {
            Column c = i.next();
            SQL += (comma ? ", " : "") + c.getName() + " " + columnToTypeString(c) + (c.isNotNull() ? " NOT NULL" : " NULL");

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
                SQL += "UNIQUE KEY (";
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
        if (tableType != null && tableType.length() > 0)
            SQL += " ENGINE=" + tableType;
        SQL += " CHARSET=utf8";
        execute(SQL);
    }

    @Override
    public String getDatabaseObjectName(String name) {
        return "`" + name + "`";
    }

    @Override
    public SqlOperationMatcher getSqlNameMatcher() throws ReplicatorException {
        return new MySQLOperationMatcher();
    }
}
