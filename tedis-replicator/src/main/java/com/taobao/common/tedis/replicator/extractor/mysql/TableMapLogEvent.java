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

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.extractor.mysql.conversion.BigEndianConversion;
import com.taobao.common.tedis.replicator.extractor.mysql.conversion.LittleEndianConversion;

public class TableMapLogEvent extends LogEvent {
    /**
     * Fixed data part:
     * <ul>
     * <li>6 bytes. The table ID.</li>
     * <li>2 bytes. Reserved for future use.</li>
     * </ul>
     * <p>
     * Variable data part:
     * <ul>
     * <li>1 byte. The length of the database name.</li>
     * <li>Variable-sized. The database name (null-terminated).</li>
     * <li>1 byte. The length of the table name.</li>
     * <li>Variable-sized. The table name (null-terminated).</li>
     * <li>Packed integer. The number of columns in the table.</li>
     * <li>Variable-sized. An array of column types, one byte per column.</li>
     * <li>Packed integer. The length of the metadata block.</li>
     * <li>Variable-sized. The metadata block; see log_event.h for contents and
     * format.</li>
     * <li>Variable-sized. Bit-field indicating whether each column can be NULL,
     * one bit per column. For this field, the amount of storage required for N
     * columns is INT((N+7)/8) bytes.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */
    static Logger logger = Logger.getLogger(MySQLExtractor.class);

    private long tableId;
    private int databaseNameLength;
    private int tableNameLength;
    private long columnsCount;

    private String databaseName;
    private String tableName;

    private byte[] columnsTypes;

    // Not used for now...
    // private String nullBits;

    private int[] metadata;
    private int metadataSize;

    public long getTableId() {
        return tableId;
    }

    public long getColumnsCount() {
        return columnsCount;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public byte[] getColumnsTypes() {
        return columnsTypes;
    }

    public int[] getMetadata() {
        return metadata;
    }

    private void buildMetadata(byte[] fieldMetadata, int pos, int length) throws IOException {
        int index = pos;
        for (int i = 0; i < columnsCount; i++) {
            int columnType = LittleEndianConversion.convert1ByteToInt(columnsTypes, i);
            switch (columnType) {
            case MysqlBinlog.MYSQL_TYPE_TINY_BLOB:
            case MysqlBinlog.MYSQL_TYPE_BLOB:
            case MysqlBinlog.MYSQL_TYPE_MEDIUM_BLOB:
            case MysqlBinlog.MYSQL_TYPE_LONG_BLOB:
            case MysqlBinlog.MYSQL_TYPE_DOUBLE:
            case MysqlBinlog.MYSQL_TYPE_FLOAT:
                /* These types store a single byte */
                metadata[i] = fieldMetadata[index];
                index++;
                break;

            case MysqlBinlog.MYSQL_TYPE_SET:
            case MysqlBinlog.MYSQL_TYPE_ENUM:
                /*
                 * log_event.h : MYSQL_TYPE_SET & MYSQL_TYPE_ENUM : This
                 * enumeration value is only used internally and cannot exist in
                 * a binlog.
                 */
            case MysqlBinlog.MYSQL_TYPE_STRING: {
                /*
                 * log_event.h : The first byte is always MYSQL_TYPE_VAR_STRING
                 * (i.e., 253). The second byte is the field size, i.e., the
                 * number of bytes in the representation of size of the string:
                 * 3 or 4.
                 */
                int x = BigEndianConversion.convert2BytesToInt(fieldMetadata, index);
                metadata[i] = x;
                index += 2;
                break;
            }
            case MysqlBinlog.MYSQL_TYPE_BIT: {
                int x = LittleEndianConversion.convert2BytesToInt(fieldMetadata, index);
                metadata[i] = x;
                metadata[i] = x;
                index += 2;
                break;
            }
            case MysqlBinlog.MYSQL_TYPE_VARCHAR: {
                /*
                 * These types store two bytes.
                 */
                metadata[i] = LittleEndianConversion.convert2BytesToInt(fieldMetadata, index);
                index = index + 2;
                break;
            }
            case MysqlBinlog.MYSQL_TYPE_NEWDECIMAL: {
                int x = BigEndianConversion.convert2bytesToShort(fieldMetadata, index);
                metadata[i] = x;
                index += 2;
                break;
            }
            default:
                metadata[i] = 0;
                break;
            }
            if (logger.isDebugEnabled())
                logger.debug("column: " + i + " type: " + columnType + " length: " + metadata[i]);
        }
    }

    public TableMapLogEvent(byte[] buffer, int eventLength, FormatDescriptionLogEvent descriptionEvent) throws ReplicatorException {
        super(buffer, descriptionEvent, MysqlBinlog.TABLE_MAP_EVENT);

        int commonHeaderLength, postHeaderLength;

        int postHeaderIndex;

        commonHeaderLength = descriptionEvent.commonHeaderLength;
        postHeaderLength = descriptionEvent.postHeaderLength[type - 1];

        if (logger.isDebugEnabled())
            logger.debug("event length: " + eventLength + " common header length: " + commonHeaderLength + " post_header_len: " + postHeaderLength);

        try {
            /* Read the post-header */
            postHeaderIndex = commonHeaderLength;

            postHeaderIndex += MysqlBinlog.TM_MAPID_OFFSET;
            if (postHeaderLength == 6) {
                /*
                 * Master is of an intermediate source tree before 5.1.4. Id is
                 * 4 bytes
                 */
                tableId = LittleEndianConversion.convert4BytesToLong(buffer, postHeaderIndex);
                postHeaderIndex += 4;
            } else {
                assert (postHeaderLength == MysqlBinlog.TABLE_MAP_HEADER_LEN);
                tableId = LittleEndianConversion.convert6BytesToLong(buffer, postHeaderIndex);
                postHeaderIndex += MysqlBinlog.TM_FLAGS_OFFSET;
            }

            /*
             * Next 2 bytes are reserved for future use : no need to process
             * them for now.
             */

            /* Read the variable data part of the event */
            int variableStartIndex = commonHeaderLength + postHeaderLength;

            /* Extract the length of the various parts from the buffer */
            int index = variableStartIndex + 0;
            databaseNameLength = LittleEndianConversion.convert1ByteToInt(buffer, index);
            index++;
            databaseName = new String(buffer, index, databaseNameLength);

            /* Length of database name + terminating null */
            index += databaseNameLength + 1;

            // int ptr_tbllen = index + databaseNameLength + 2;
            tableNameLength = LittleEndianConversion.convert1ByteToInt(buffer, index);
            index++;
            tableName = new String(buffer, index, tableNameLength);

            /* Length of table name + terminating null */
            index += tableNameLength + 1;

            long ret[] = MysqlBinlog.decodePackedInteger(buffer, index);
            columnsCount = ret[0];
            index = (int) ret[1];

            if (logger.isDebugEnabled())
                logger.debug("Db Name : " + databaseName + " (" + databaseNameLength + ")" + " Tablename : " + tableName + " (" + tableNameLength + ")" + " Columns count: " + columnsCount);

            columnsTypes = new byte[(int) columnsCount];
            System.arraycopy(buffer, index, columnsTypes, 0, (int) columnsCount);

            index += columnsCount;

            if (logger.isDebugEnabled())
                logger.debug("Bytes read: " + index);

            /* initialize field metadata according to table column count */
            metadataSize = (int) columnsCount * 2;
            metadata = new int[metadataSize];
            for (int i = 0; i < columnsCount * 2; i++)
                metadata[i] = 0;

            if (index < eventLength) {
                ret = MysqlBinlog.decodePackedInteger(buffer, index);
                int metadata_size = (int) ret[0];
                index = (int) ret[1];
                assert (metadata_size <= (columnsCount * 2));

                buildMetadata(buffer, index, metadata_size);
                index += metadata_size;

                // For now, the following is not used
                // int nullBytesCount = ((int) columnsCount + 7) / 8;
                // nullBits = new String(buffer, index,
                // nullBytesCount);
            }
        } catch (IOException e) {
            logger.error("Table Map event parsing failed for: " + e);
        }
        return;
    }
}
