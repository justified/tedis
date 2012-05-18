/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import java.io.IOException;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.extractor.mysql.conversion.LittleEndianConversion;

public class XidLogEvent extends LogEvent {
    long xid;

    public XidLogEvent(byte[] buffer, int eventLength, FormatDescriptionLogEvent descriptionEvent) throws ReplicatorException {
        super(buffer, descriptionEvent, MysqlBinlog.XID_EVENT);
        try {
            xid = LittleEndianConversion.convert8BytesToLong(buffer, descriptionEvent.commonHeaderLength);
        } catch (IOException e) {
            throw new MySQLExtractException("could not extract trx id", e);
        }
    }

    public long getXid() {
        return xid;
    }
}
