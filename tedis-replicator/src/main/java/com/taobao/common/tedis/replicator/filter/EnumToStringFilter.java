/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.filter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.data.DBMSData;
import com.taobao.common.tedis.replicator.data.OneRowChange;
import com.taobao.common.tedis.replicator.data.RowChangeData;
import com.taobao.common.tedis.replicator.data.StatementData;
import com.taobao.common.tedis.replicator.data.OneRowChange.ColumnSpec;
import com.taobao.common.tedis.replicator.data.OneRowChange.ColumnVal;
import com.taobao.common.tedis.replicator.database.Column;
import com.taobao.common.tedis.replicator.database.Database;
import com.taobao.common.tedis.replicator.database.DatabaseFactory;
import com.taobao.common.tedis.replicator.database.MySQLOperationMatcher;
import com.taobao.common.tedis.replicator.database.SqlOperation;
import com.taobao.common.tedis.replicator.database.SqlOperationMatcher;
import com.taobao.common.tedis.replicator.database.Table;
import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.plugin.PluginContext;

/**
 * EnumToStringFilter transforms enum data type values to corresponding string
 * representation as follows:<br/>
 * 1. On each event it checks whether targeted table has enum data type column.<br/>
 * 2. If it does, corresponding enum column values of the event are mapped from
 * integer into string representations.<br/>
 * <br/>
 * The filter is to be used with row replication.<br/>
 * <br/>
 * Filter takes an optional parameter for performance tuning. Instead of
 * checking all the tables you may define only a specific comma-delimited list
 * in process_tables_schemas parameter. Eg.:<br/>
 * replicator.filter.enumtostringfilter.process_tables_schemas=myschema.mytable1
 * ,myschema.mytable2
 */
public class EnumToStringFilter implements Filter {
    private class TableWithEnums {
        Table table = null;
        HashMap<Integer, String[]> enumDefinitions = null;

        TableWithEnums(Table table) {
            this.table = table;
        }

        public Table getTable() {
            return this.table;
        }

        public HashMap<Integer, String[]> getEnumDefinitions() {
            return this.enumDefinitions;
        }

        public void setEnumDefinitions(HashMap<Integer, String[]> enumDefinitions) {
            this.enumDefinitions = enumDefinitions;
        }
    }

    private static Logger logger = Logger.getLogger(EnumToStringFilter.class);

    // Metadata cache is a hashtable indexed by the database name and each
    // database uses a hashtable indexed by the table name (This is done in
    // order to be able to drop all table definitions at once if a DROP DATABASE
    // is trapped). Filling metadata cache is done in a lazy way. It will be
    // updated only when a table is used for the first time by a row event.
    private Hashtable<String, Hashtable<String, TableWithEnums>> metadataCache;

    Database conn = null;

    private String user;
    private String url;
    private String password;

    private List<String> tables = null;
    private List<String> schemas = null;
    private String processTablesSchemas = null;

    // SQL parser.
    SqlOperationMatcher sqlMatcher = new MySQLOperationMatcher();

    public void configure(PluginContext context) throws ReplicatorException {
        if (processTablesSchemas != null) {
            if (logger.isDebugEnabled())
                logger.debug("Tables to process: " + processTablesSchemas);

            tables = new ArrayList<String>();
            schemas = new ArrayList<String>();

            String[] list = processTablesSchemas.split(",");
            for (int i = 0; i < list.length; i++) {
                String t = list[i].trim().toUpperCase();
                if (t.contains("."))
                    tables.add(t);
                else
                    schemas.add(t);
            }
        }
    }

    public void prepare(PluginContext context) throws ReplicatorException {
        metadataCache = new Hashtable<String, Hashtable<String, TableWithEnums>>();

        // Load defaults for connection
        if (url == null)
            url = context.getJdbcUrl("tedis");
        if (user == null)
            user = context.getJdbcUser();
        if (password == null)
            password = context.getJdbcPassword();

        // Connect.
        try {
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect();
        } catch (SQLException e) {
            throw new ReplicatorException(e);
        }
    }

    public void release(PluginContext context) throws ReplicatorException {
        if (metadataCache != null) {
            metadataCache.clear();
            metadataCache = null;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException, InterruptedException {
        ArrayList<DBMSData> data = event.getData();
        if (data == null)
            return event;
        for (DBMSData dataElem : data) {
            if (dataElem instanceof RowChangeData) {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (OneRowChange orc : rdata.getRowChanges())
                    try {
                        checkForEnum(orc);
                    } catch (SQLException e) {
                        throw new ReplicatorException("Filter failed processing primary key information", e);
                    }
            } else if (dataElem instanceof StatementData) {
                StatementData sdata = (StatementData) dataElem;
                // Parse statements in order to update table definitions if
                // needed. e.g. DROP DATABASE should drop information about keys
                // which are defined for this database tables, ...
                String query = sdata.getQuery();
                if (query == null)
                    query = new String(sdata.getQueryAsBytes());

                SqlOperation sqlOperation = sqlMatcher.match(query);

                if (sqlOperation.getOperation() == SqlOperation.DROP && sqlOperation.getObjectType() == SqlOperation.SCHEMA) {
                    // "drop database" statement detected : remove database
                    // metadata
                    String dbName = sqlOperation.getSchema();
                    if (metadataCache.remove(dbName) != null) {
                        if (logger.isDebugEnabled())
                            logger.debug("DROP DATABASE detected - Removing database metadata for '" + dbName + "'");
                    } else if (logger.isDebugEnabled())
                        logger.debug("DROP DATABASE detected - no cached database metadata to delete for '" + dbName + "'");
                    continue;
                } else if (sqlOperation.getOperation() == SqlOperation.ALTER) {
                    // Detected an alter table statement / Dropping table
                    // metadata for the concerned table
                    String name = sqlOperation.getName();
                    String defaultDB = sdata.getDefaultSchema();
                    removeTableMetadata(name, sqlOperation.getSchema(), defaultDB);
                    continue;
                }

            }
        }
        return event;
    }

    private void removeTableMetadata(String tableName, String schemaName, String defaultDB) {
        if (schemaName != null) {
            Hashtable<String, TableWithEnums> tableCache = metadataCache.get(schemaName);
            if (tableCache != null && tableCache.remove(tableName) != null) {
                if (logger.isDebugEnabled())
                    logger.debug("ALTER TABLE detected - Removing table metadata for '" + schemaName + "." + tableName + "'");
            } else if (logger.isDebugEnabled())
                logger.debug("ALTER TABLE detected - no cached table metadata to remove for '" + schemaName + "." + tableName + "'");
        } else {
            Hashtable<String, TableWithEnums> tableCache = metadataCache.get(defaultDB);
            if (tableCache != null && tableCache.remove(tableName) != null)
                logger.info("ALTER TABLE detected - Removing table metadata for '" + defaultDB + "." + tableName + "'");
            else
                logger.info("ALTER TABLE detected - no cached table metadata to remove for '" + defaultDB + "." + tableName + "'");
        }
    }

    /**
     * Parses MySQL enum type definition statement. Eg.:<br/>
     * enum('Active','Inactive','Removed')<br/>
     * enum('No','Yes')<br/>
     * etc.<br/>
     *
     * @param enumDefinition
     *            String of the following form: enum('val1','val2',...)
     * @return Enumeration elements in an array. Unquoted. Eg.:
     *         Active,Inactive,Removed
     */
    private String[] parseEnumeration(String enumDefinition) {
        // Parse out what's inside brackets.
        String keyword = "enum(";
        int iA = enumDefinition.indexOf(keyword);
        int iB = enumDefinition.indexOf(')', iA);
        String list = enumDefinition.substring(iA + keyword.length(), iB);

        // Split by comma, remove quotes and save into array.
        String[] listArray = list.split(",");
        String[] enumElements = new String[listArray.length];
        for (int i = 0; i < listArray.length; i++) {
            String elementQuoted = listArray[i];
            String element = elementQuoted.substring(1, elementQuoted.length() - 1);
            enumElements[i] = element;
        }

        return enumElements;
    }

    /**
     * Connects to the MySQL database, executes SHOW COLUMNS for a defined table
     * and parses out allowed ENUM values to their corresponding String
     * representations.
     *
     * @param schemaTable
     *            String in form of "schema.table" to execute SHOW COLUMNS
     *            against.
     * @param column
     *            ENUM type column to retrieve values about.
     * @return Array of allowed ENUM values for the column. NOTE: index, as in
     *         arrays, starts from zero (0), but MySQL ENUM values' numeric
     *         representation starts from one (1) - thus, make sure you retrieve
     *         the correct value from this array by shifting the key by one,
     *         i.e.: [MySQLENUMValueNumeric - 1]
     * @throws SQLException
     */
    private String[] retrieveEnumeration(String schemaTable, String column) throws SQLException {
        String[] enumElements = null;

        // Get the allowed enum values.
        String query = "SHOW COLUMNS FROM " + schemaTable + " WHERE Field='" + column + "'";
        Statement st = null;
        ResultSet rs = null;
        try {
            st = conn.createStatement();
            rs = st.executeQuery(query);
            if (rs.next()) {
                String enumDefinition = rs.getString("Type");
                if (logger.isDebugEnabled())
                    logger.debug(enumDefinition);

                enumElements = parseEnumeration(enumDefinition);
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException e) {
                }
            }
        }

        return enumElements;
    }

    /**
     * Checks for enum columns in the event. If found, transforms values from
     * integers to corresponding strings.
     */
    private void checkForEnum(OneRowChange orc) throws SQLException, ReplicatorException {
        String tableName = orc.getTableName();

        // Check only a specific list of tables?
        if (schemas != null && (!schemas.contains(orc.getSchemaName().toUpperCase()) && !tables.contains((orc.getSchemaName() + "." + tableName).toUpperCase()))) {
            if (logger.isInfoEnabled())
                logger.info("Table " + orc.getSchemaName() + "." + tableName + " not taken into account by the EnumToStringFilter");
            return;
        }

        if (!metadataCache.containsKey(orc.getSchemaName())) {
            // Nothing defined yet in this database
            metadataCache.put(orc.getSchemaName(), new Hashtable<String, TableWithEnums>());
        }

        Hashtable<String, TableWithEnums> dbCache = metadataCache.get(orc.getSchemaName());

        if (!dbCache.containsKey(tableName) || orc.getTableId() == -1 || dbCache.get(tableName).getTable() == null || dbCache.get(tableName).getTable().getTableId() != orc.getTableId()) {
            // This table was not processed yet or schema changed since it was
            // cached : fetch information about its primary key
            if (dbCache.remove(tableName) != null && logger.isDebugEnabled())
                logger.debug("Detected a schema change for table " + orc.getSchemaName() + "." + tableName + " - Removing table metadata from cache");
            Table newTable = conn.findTable(orc.getSchemaName(), orc.getTableName());
            newTable.setTableId(orc.getTableId());
            dbCache.put(tableName, new TableWithEnums(newTable));
        }

        // Is there any enum columns in this table? If so, retrieve enum
        // definitions of each enum column.
        TableWithEnums table = dbCache.get(tableName);
        // Have we already cached enum definitions?
        HashMap<Integer, String[]> enumDefinitions = table.getEnumDefinitions();
        if (enumDefinitions != null) {
            if (logger.isDebugEnabled())
                logger.debug("Using ENUM cache (columns: " + enumDefinitions.size() + ") @ " + table.getTable().getSchema() + "." + table.getTable().getName());
        } else {
            // Enum definitions not in cache, retrieve & cache.
            enumDefinitions = new HashMap<Integer, String[]>();
            for (Column col : table.getTable().getAllColumns()) {
                if (col.getTypeDescription() != null) {
                    if (col.getTypeDescription().startsWith("ENUM")) {
                        if (logger.isDebugEnabled())
                            logger.debug("ENUM @ " + col.getPosition() + " : " + table.getTable().getSchema() + "." + table.getTable().getName() + "." + col.getName());
                        String[] enumDefinition = retrieveEnumeration(table.getTable().getSchema() + "." + table.getTable().getName(), col.getName());
                        if (enumDefinition == null) {
                            logger.error("Failed to retrieve enumeration definition for " + table.getTable().getSchema() + "." + table.getTable().getName() + "." + col.getName());
                            return;
                        } else
                            enumDefinitions.put(col.getPosition(), enumDefinition);
                    }
                } else
                    logger.error("Column type description is null for " + table.getTable().getName() + "." + col.getName());
            }
            // Cache the retrieved definitions.
            if (logger.isDebugEnabled())
                logger.debug("Saving ENUM definitions (columns: " + enumDefinitions.size() + ") to cache @ " + table.getTable().getSchema() + "." + table.getTable().getName());
            table.setEnumDefinitions(enumDefinitions);
        }
        if (enumDefinitions.size() == 0) {
            if (logger.isDebugEnabled())
                logger.debug("No ENUM columns @ " + table.getTable().getSchema() + "." + table.getTable().getName());
            return;
        }

        // Table columns of enum type identified.
        // 1. Transform event's columns.
        ArrayList<ColumnSpec> columns = orc.getColumnSpec();
        ArrayList<ArrayList<ColumnVal>> columnValues = orc.getColumnValues();
        transformColumns(columns, columnValues, enumDefinitions, "COL");
        // 2. Transform event's keys.
        ArrayList<ColumnSpec> keys = orc.getKeySpec();
        ArrayList<ArrayList<ColumnVal>> keyValues = orc.getKeyValues();
        transformColumns(keys, keyValues, enumDefinitions, "KEY");
    }

    private void transformColumns(ArrayList<ColumnSpec> columns, ArrayList<ArrayList<ColumnVal>> columnValues, HashMap<Integer, String[]> enumDefinitions, String typeCaption)
            throws ReplicatorException {
        // Looping through all and checking the real underlying index of each,
        // because there might be gaps as an outcome of some other filters.
        for (int c = 0; c < columns.size(); c++) {
            ColumnSpec colSpec = columns.get(c);
            if (enumDefinitions.containsKey(colSpec.getIndex())) {
                if (logger.isDebugEnabled())
                    logger.debug("Transforming " + typeCaption + "(" + colSpec.getIndex() + ")");
                if (colSpec.getType() == java.sql.Types.OTHER /* ENUM */
                        || colSpec.getType() == java.sql.Types.NULL) {
                    // Change the underlying type in the event.
                    colSpec.setType(java.sql.Types.VARCHAR);

                    // Iterate through all rows in the event and transform each.
                    for (int row = 0; row < columnValues.size(); row++) {
                        // ColumnVal keyValue = keyValues.get(row).get(k);
                        ColumnVal colValue = columnValues.get(row).get(c);
                        // It must be integer at this point.
                        if (colValue.getValue() != null) {
                            int currentValue = (Integer) colValue.getValue();
                            String enumDefs[] = enumDefinitions.get(colSpec.getIndex());
                            String newValue = null;
                            if (currentValue > enumDefs.length) {
                                throw new ReplicatorException("MySQL value (" + currentValue + ") for ENUM @ Col " + colSpec.getIndex() + " Row " + row + " is greater than available values ("
                                        + enumDefs.length + ")");
                            } else if (currentValue == 0) {
                                // MySQL stores an empty string in enum 0:
                                newValue = "";
                            } else
                                newValue = enumDefs[currentValue - 1];
                            colValue.setValue(newValue);
                            if (logger.isDebugEnabled())
                                logger.debug("Col " + colSpec.getIndex() + " Row " + row + ": " + currentValue + " -> " + newValue);
                        } else {
                            if (logger.isDebugEnabled())
                                logger.debug("Col " + colSpec.getIndex() + " Row " + row + ": null");
                        }
                    }
                } else if (colSpec.getType() == java.sql.Types.VARCHAR)
                    logger.warn("Column type is already VARCHAR! Assuming it is because this event was already transformed by this filter. Ignoring this column");
                else
                    logger.error("Unexpected column type (" + colSpec.getType() + ") in supposedly ENUM column! Ignoring this column");
            }
        }
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setProcessTablesSchemas(String processTablesSchemas) {
        this.processTablesSchemas = processTablesSchemas;
    }

}
