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

public class DeleteFileLogEvent extends LogEvent {
    int fileID;

    public DeleteFileLogEvent(byte[] buffer, int eventLength, FormatDescriptionLogEvent descriptionEvent) throws ReplicatorException {
        super(buffer, descriptionEvent, MysqlBinlog.DELETE_FILE_EVENT);

        int commonHeaderLength, postHeaderLength;

        int fixedPartIndex;

        commonHeaderLength = descriptionEvent.commonHeaderLength;
        postHeaderLength = descriptionEvent.postHeaderLength[type - 1];

        if (logger.isDebugEnabled())
            logger.debug("event length: " + eventLength + " common header length: " + commonHeaderLength + " post header length: " + postHeaderLength);

        /* Read the fixed data part */
        fixedPartIndex = commonHeaderLength;

        try {
            /* 4 Bytes for file ID */
            fileID = LittleEndianConversion.convert4BytesToInt(buffer, fixedPartIndex);

        } catch (IOException e) {
            logger.error("Rows log event parsing failed : ", e);
        }
    }

    public int getFileID() {
        return fileID;
    }
}
