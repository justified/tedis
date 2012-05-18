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

public class ExecuteLoadQueryLogEvent extends QueryLogEvent {

    /**
     * <ul>
     * <li>4 bytes. The ID of the file to load.</li>
     * <li>4 bytes. The start position within the statement for filename
     * substitution.</li>
     * <li>4 bytes. The end position within the statement for filename
     * substitution.</li>
     * <li>1 byte. How to handle duplicates: LOAD_DUP_ERROR = 0, LOAD_DUP_IGNORE
     * = 1, LOAD_DUP_REPLACE = 2</li>
     * </ul>
     */

    private int fileID;
    private int filenameStartPos;
    private int filenameEndPos;

    public ExecuteLoadQueryLogEvent(byte[] buffer, int eventLength, FormatDescriptionLogEvent descriptionEvent, boolean parseStatements) throws ReplicatorException {
        super(buffer, descriptionEvent, MysqlBinlog.EXECUTE_LOAD_QUERY_EVENT);

        this.parseStatements = parseStatements;

        int dataLength;
        int commonHeaderLength, postHeaderLength;
        int start;
        int end;
        int databaseNameLength;

        commonHeaderLength = descriptionEvent.commonHeaderLength;
        postHeaderLength = descriptionEvent.postHeaderLength[type - 1];

        if (logger.isDebugEnabled())
            logger.debug("event length: " + eventLength + " common header length: " + commonHeaderLength + " post header length: " + postHeaderLength);

        if (eventLength < commonHeaderLength + postHeaderLength) {
            logger.warn("query event length is too short");
            throw new MySQLExtractException("too short query event");
        }

        dataLength = eventLength - (commonHeaderLength + postHeaderLength);

        int index = commonHeaderLength;
        int statusVariablesLength = 0;
        try {
            index += MysqlBinlog.Q_THREAD_ID_OFFSET;
            threadId = LittleEndianConversion.convert4BytesToLong(buffer, index);

            index += 4; // commonHeaderLength + MysqlBinlog.Q_EXEC_TIME_OFFSET
            execTime = LittleEndianConversion.convert4BytesToLong(buffer, index);

            index += 4; // commonHeaderLength + MysqlBinlog.Q_DB_LEN_OFFSET
            databaseNameLength = LittleEndianConversion.convert1ByteToInt(buffer, index);

            index++; // commonHeaderLength + MysqlBinlog.Q_ERR_CODE_OFFSET
            errorCode = LittleEndianConversion.convert2BytesToInt(buffer, index);

            // TODO: add a check of all *_len vars
            index += 2; // commonHeaderLength +
            // MysqlBinlog.Q_STATUS_VARS_LEN_OFFSET
            statusVariablesLength = LittleEndianConversion.convert2BytesToInt(buffer, index);
            index += 2;

            dataLength -= statusVariablesLength;
            if (logger.isDebugEnabled())
                logger.debug("QueryLogEvent has statusVariablesLength : " + statusVariablesLength);
        } catch (IOException e) {
            throw new MySQLExtractException("query event header parsing failed");
        }

        try {
            fileID = LittleEndianConversion.convert4BytesToInt(buffer, index);
            index += 4;

            filenameStartPos = LittleEndianConversion.convert4BytesToInt(buffer, index);
            index += 4;

            filenameEndPos = LittleEndianConversion.convert4BytesToInt(buffer, index);
            index += 4;

        } catch (IOException e) {
        }

        start = commonHeaderLength + postHeaderLength;
        end = start + statusVariablesLength;
        extractStatusVariables(buffer, start, end);

        index = end;
        databaseName = new String(buffer, index, databaseNameLength);
        index += databaseNameLength + 1;
        dataLength -= databaseNameLength + 1;
        query = new String(buffer, index, dataLength);

        if (charset_inited) {
            // 6 byte character set flag:
            // 1-2 = character set client
            // 3-4 = collation client
            // 5-6 = collation server
            try {
                clientCharsetId = LittleEndianConversion.convert2BytesToInt(charset, 0);
                clientCollationId = LittleEndianConversion.convert2BytesToInt(charset, 2);
                serverCollationId = LittleEndianConversion.convert2BytesToInt(charset, 4);
            } catch (IOException e) {
                logger.error("failed to use character id: " + charset);
            }
        }

    }

    public int getEndPos() {
        return filenameEndPos;
    }

    public int getStartPos() {
        return filenameStartPos;
    }

    public int getFileID() {
        return fileID;
    }
}
