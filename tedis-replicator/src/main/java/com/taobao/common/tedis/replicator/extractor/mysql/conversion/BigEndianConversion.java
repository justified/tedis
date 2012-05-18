/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql.conversion;

import com.taobao.common.tedis.replicator.extractor.mysql.MysqlBinlog;

public class BigEndianConversion extends GeneralConversion {

    /* Reads big-endian integer from no more than 4 bytes */
    public static int convertNBytesToInt(byte[] buffer, int offset, int length) {
        int ret = 0;
        for (int i = offset; i < (offset + length); i++) {
            ret = (ret << 8) | unsignedByteToInt(buffer[i]);
        }
        return ret;
    }

    public static String convertNBytesToString(byte[] buffer, int offset, int length) {
        String ret = "";
        int i = offset;
        int end = offset + length;

        while (i < end) {
            if ((end - i) > MysqlBinlog.SIZE_OF_INT32) {
                // whole integer parts
                int int32 = convertNBytesToInt(buffer, i, MysqlBinlog.SIZE_OF_INT32);
                ret = ret + String.format("%09d", int32);
                i = i + MysqlBinlog.SIZE_OF_INT32;
            } else {
                // last integer part bytes
                int int32 = convertNBytesToInt(buffer, i, end - i);
                ret = ret + int32;
                i = end; // we're done with int part
            }
        }
        return ret;
    }

    public static int convert1ByteToInt(byte[] buffer, int offset) {
        return unsignedByteToInt(buffer[offset]);
    }

    public static int convert2BytesToInt(byte[] buffer, int offset) {
        int value;
        value = unsignedByteToInt(buffer[offset + 1]);
        value += unsignedByteToInt(buffer[offset]) << 8;
        return value;
    }

    public static int convert3BytesToInt(byte[] buffer, int offset) {
        int value;
        value = ((buffer[offset + 0] & 128) != 128) ? (((unsignedByteToInt(buffer[offset]) << 16) | (unsignedByteToInt(buffer[offset + 1]) << 8) | unsignedByteToInt(buffer[offset + 2])))
                : ((255 << 24) | (unsignedByteToInt(buffer[offset]) << 16) | (unsignedByteToInt(buffer[offset + 1]) << 8) | unsignedByteToInt(buffer[offset + 2]));
        return value;
    }

    public static int convert4BytesToInt(byte[] buffer, int offset) {
        int value;
        value = unsignedByteToInt(buffer[offset + 3]);
        value += unsignedByteToInt(buffer[offset + 2]) << 8;
        value += unsignedByteToInt(buffer[offset + 1]) << 16;
        value += unsignedByteToInt(buffer[offset]) << 24;
        return value;
    }

    public static long convertNBytesToLong(byte[] buffer, int offset, int length) {
        long ret = 0;
        for (int i = offset; i < (offset + length); i++) {
            ret = (ret << 8) + unsignedByteToInt(buffer[i]);
        }
        return ret;
    }

    public static short convert1ByteToShort(byte[] buffer, int offset) {
        short value;
        value = (short) buffer[offset + 0];
        return value;
    }

    public static short convert2bytesToShort(byte[] buffer, int offset) {
        short value;
        value = (short) ((short) buffer[offset + 0] << 8);
        value += (short) buffer[offset + 1];
        return value;
    }

}
