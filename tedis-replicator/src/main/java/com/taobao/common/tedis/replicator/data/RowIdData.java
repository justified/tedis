/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.data;

public class RowIdData extends DBMSData {
    private static final long serialVersionUID = 1L;

    public static final int LAST_INSERT_ID = 1;
    public static final int INSERT_ID = 2;

    private long rowId;
    private int type;

    public RowIdData(long value, int type) {
        this.rowId = value;
        this.type = type;
    }

    public long getRowId() {
        return rowId;
    }

    public int getType() {
        return type;
    }

}
