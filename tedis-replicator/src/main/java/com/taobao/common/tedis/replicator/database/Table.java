/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

import java.util.ArrayList;
import java.util.Iterator;
import java.sql.PreparedStatement;

public class Table {

    String schema = null;
    String name = null;
    ArrayList<Column> allColumns = null;
    ArrayList<Column> nonKeyColumns = null;
    ArrayList<Key> keys = null;
    Key primaryKey = null;
    PreparedStatement statements[];
    // Cache of prepared statements
    boolean cacheStatements = false;

    static public final int INSERT = 0;
    static public final int UPDATE1 = 1;
    static public final int UPDATE2 = 2;
    static public final int DELETE = 3;
    static public final int NPREPPED = 4;

    // scn is eventually used for caching purpose. This table object will be
    // cached and reused if possible.
    private String scn;

    // tableId as found in MySQL binlog can be used to detect schema changes.
    // Here, it has the same purpose as previous scn field
    private long tableId;

    /**
     * Creates a new <code>Table</code> object
     */
    public Table(String schema, String name) {
        int i;

        this.schema = schema;
        this.name = name;
        this.allColumns = new ArrayList<Column>();
        this.nonKeyColumns = new ArrayList<Column>();
        this.keys = new ArrayList<Key>();
        this.scn = null;
        this.tableId = -1;
        this.statements = new PreparedStatement[Table.NPREPPED];
        this.cacheStatements = false;
        // Following probably not needed
        for (i = 0; i < Table.NPREPPED; i++)
            this.statements[i] = null;
    }

    public Table(String schema, String name, boolean cacheStatements) {
        this(schema, name);
        this.cacheStatements = cacheStatements;
    }

    public boolean getCacheStatements() {
        return this.cacheStatements;
    }

    void purge(ArrayList<Column> purgeValues, ArrayList<Column> fromList) {
        int idx;
        Iterator<Column> i = purgeValues.iterator();
        while (i.hasNext()) {
            Column c1 = i.next();
            if ((idx = fromList.indexOf(c1)) == -1)
                continue;
            fromList.remove(idx);
        }
    }

    public PreparedStatement getStatement(int statementNumber) {
        return this.statements[statementNumber];
    }

    public void setStatement(int statementNumber, PreparedStatement statement) {
        // This will leak prepared statements if a statement already
        // exists in the slot but I currently do not want to
        // have a "Table" know about a "Database" which is what we would need
        // to close these statements.
        this.statements[statementNumber] = statement;
    }

    public void AddColumn(Column column) {
        allColumns.add(column);
        nonKeyColumns.add(column);
    }

    public void AddKey(Key key) {
        keys.add(key);
        if (key.getType() == Key.Primary) {
            primaryKey = key;
            purge(key.getColumns(), nonKeyColumns);
        }
    }

    public void Dump() {
        System.out.format("%s.%s\n", this.schema, this.name);
        Iterator<Column> i = allColumns.iterator();
        while (i.hasNext()) {
            Column c = i.next();
            c.Dump();
        }
    }

    public String getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }

    public ArrayList<Column> getAllColumns() {
        return allColumns;
    }

    public ArrayList<Column> getNonKeyColumns() {
        return nonKeyColumns;
    }

    public ArrayList<Key> getKeys() {
        return keys;
    }

    public Key getPrimaryKey() {
        return primaryKey;
    }

    /*
     * columnNumbers here are one based. perhaps we should record the column
     * number in the Column class as well.
     */
    public Column findColumn(int columnNumber) {
        /* This assumes column were added in column number order */
        return allColumns.get(columnNumber - 1);
    }

    public int getColumnCount() {
        return allColumns.size();
    }

    public String getSCN() {
        return scn;
    }

    public void setSCN(String scn) {
        this.scn = scn;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    public String toString() {
        return this.schema + "." + this.name;
    }
}
