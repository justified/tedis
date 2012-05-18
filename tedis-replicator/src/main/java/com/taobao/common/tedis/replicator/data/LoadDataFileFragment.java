/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.data;

public class LoadDataFileFragment extends DBMSData {

    private int fileId;
    private byte[] data;
    private String defaultSchema;

    public LoadDataFileFragment(int fileID, byte[] data) {
        this.fileId = fileID;
        this.data = data;
    }

    public LoadDataFileFragment(int fileId, byte[] data, String schema) {
        super();
        this.fileId = fileId;
        this.data = data;
        this.defaultSchema = schema;
    }

    private static final long serialVersionUID = 1L;

    public int getFileID() {
        return fileId;
    }

    public byte[] getData() {
        return data;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }
}
