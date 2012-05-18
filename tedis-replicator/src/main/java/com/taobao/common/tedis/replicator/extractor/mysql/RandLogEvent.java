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

public class RandLogEvent extends LogEvent {
    /**
     * Fixed data part: Empty
     * <p>
     * Variable data part:
     * <ul>
     * <li>8 bytes. The value for the first seed.</li>
     * <li>8 bytes. The value for the second seed.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */

    static Logger logger = Logger.getLogger(MySQLExtractor.class);

    private String query;
    private long seed1;
    private long seed2;

    public String getQuery() {
        return query;
    }

    public RandLogEvent(byte[] buffer, int eventLength, FormatDescriptionLogEvent descriptionEvent) throws ReplicatorException {
        super(buffer, descriptionEvent, MysqlBinlog.RAND_EVENT);

        int commonHeaderLength, postHeaderLength;
        int offset;

        commonHeaderLength = descriptionEvent.commonHeaderLength;
        postHeaderLength = descriptionEvent.postHeaderLength[type - 1];

        if (logger.isDebugEnabled())
            logger.debug("event length: " + eventLength + " common header length: " + commonHeaderLength + " post header length: " + postHeaderLength);

        offset = commonHeaderLength + postHeaderLength + MysqlBinlog.RAND_SEED1_OFFSET;

        /*
         * Check that the event length is greater than the calculated offset
         */
        if (eventLength < offset) {
            throw new MySQLExtractException("rand event length is too short");
        }

        try {
            seed1 = LittleEndianConversion.convert8BytesToLong(buffer, offset);
            offset = offset + MysqlBinlog.RAND_SEED2_OFFSET;
            seed2 = LittleEndianConversion.convert8BytesToLong(buffer, offset);

            query = new String("SET SESSION rand_seed1 = " + seed1 + " , rand_seed2 = " + seed2);
        } catch (Exception e) {
            throw new MySQLExtractException("Unable to read rand event", e);
        }

        return;
    }
}
