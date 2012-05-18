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

public class Key {
    public static final int IllegalType = 0;
    public static final int NonUnique = 1;
    public static final int Primary = 2;
    public static final int Unique = 3;

    int type = Key.NonUnique;
    ArrayList<Column> columns = null;

    public Key(int type) {
        this.type = type;
        this.columns = new ArrayList<Column>();
    }

    public void AddColumn(Column column) {
        columns.add(column);
    }

    public int getType() {
        return this.type;
    }

    public ArrayList<Column> getColumns() {
        return this.columns;
    }

}
