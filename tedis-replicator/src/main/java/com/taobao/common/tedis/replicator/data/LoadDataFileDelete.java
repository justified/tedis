/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.data;

public class LoadDataFileDelete extends DBMSData {
    private static final long serialVersionUID = 1L;

    private int fileId;

    public LoadDataFileDelete(int fileId) {
        this.fileId = fileId;
    }

    public int getFileID() {
        return fileId;
    }
}
