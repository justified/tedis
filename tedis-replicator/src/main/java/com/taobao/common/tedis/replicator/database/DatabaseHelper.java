/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;

import com.taobao.common.tedis.replicator.extractor.mysql.SerialBlob;

public class DatabaseHelper {
    public static SerialBlob getSafeBlob(byte[] bytes) throws SQLException {
        return getSafeBlob(bytes, 0, bytes.length);
    }

    public static SerialBlob getSafeBlob(byte[] bytes, int off, int len) throws SQLException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(bytes, off, len);
        byte[] newBytes = baos.toByteArray();
        return new SerialBlob(newBytes);
    }
}
