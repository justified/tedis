/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import java.sql.Blob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialException;

public class SerialBlob extends javax.sql.rowset.serial.SerialBlob {
    private static final long serialVersionUID = 7553956265446174128L;

    public SerialBlob(byte[] b) throws SerialException, SQLException {
        super(b);
    }

    public SerialBlob(Blob blob) throws SerialException, SQLException {
        super(blob);
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SerialException {
        if (length <= 0)
            return new byte[0];

        return super.getBytes(pos, length);
    }
}
