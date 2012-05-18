/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.extractor.mysql.conversion.LittleEndianConversion;

public class IntvarLogEvent extends LogEvent {
    /**
     * Fixed data part: Empty
     * <p>
     * Variable data part:
     * <ul>
     * <li>1 byte. A value indicating the variable type: LAST_INSERT_ID_EVENT =
     * 1 or INSERT_ID_EVENT = 2.</li>
     * <li>8 bytes. An unsigned integer indicating the value to be used for the
     * LAST_INSERT_ID() invocation or AUTO_INCREMENT column.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */

    static Logger logger = Logger.getLogger(MySQLExtractor.class);

    private long value;

    public IntvarLogEvent(byte[] buffer, int eventLength, FormatDescriptionLogEvent descriptionEvent) throws ReplicatorException {
        super(buffer, descriptionEvent, MysqlBinlog.INTVAR_EVENT);

        int commonHeaderLength, postHeaderLength;
        int offset;

        commonHeaderLength = descriptionEvent.commonHeaderLength;
        postHeaderLength = descriptionEvent.postHeaderLength[type - 1];

        if (logger.isDebugEnabled())
            logger.debug("event length: " + eventLength + " common header length: " + commonHeaderLength + " post header length: " + postHeaderLength);

        offset = commonHeaderLength + postHeaderLength + MysqlBinlog.I_TYPE_OFFSET;

        /*
         * Check that the event length is greater than the calculated offset
         */
        if (eventLength < offset) {
            throw new MySQLExtractException("INTVAR event length is too short");
        }

        try {
            type = LittleEndianConversion.convert1ByteToInt(buffer, offset);
            offset += MysqlBinlog.I_VAL_OFFSET;
            value = LittleEndianConversion.convert8BytesToLong(buffer, offset);

        } catch (Exception e) {
            throw new MySQLExtractException("Intvar extracting failed: " + e);
        }

        return;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
}
