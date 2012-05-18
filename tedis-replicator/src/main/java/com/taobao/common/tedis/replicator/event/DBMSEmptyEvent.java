/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.event;

import java.sql.Timestamp;
import java.util.ArrayList;

import com.taobao.common.tedis.replicator.data.DBMSData;

public class DBMSEmptyEvent extends DBMSEvent {
    private static final long serialVersionUID = 1300L;

    public DBMSEmptyEvent(String id, ArrayList<DBMSData> data) {
        super(id, data, new Timestamp(System.currentTimeMillis()));
    }

    public DBMSEmptyEvent(String id) {
        this(id, null);
    }
}
