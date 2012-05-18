/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.mysql;

public class MySQLConstants {
    /*
     * The following command constants are the exact counterpart of the
     * constants found in include/mysql_com.h.
     */
    public static final int COM_SLEEP = 0;

    public static final int COM_QUIT = 1;

    public static final int COM_INIT_DB = 2;

    public static final int COM_QUERY = 3;

    public static final int COM_FIELD_LIST = 4;

    public static final int COM_CREATE_DB = 5;

    public static final int COM_DROP_DB = 6;

    public static final int COM_REFRESH = 7;

    public static final int COM_SHUTDOWN = 8;

    public static final int COM_STATISTICS = 9;

    public static final int COM_PROCESS_INFO = 10;

    public static final int COM_CONNECT = 11;

    public static final int COM_PROCESS_KILL = 12;

    public static final int COM_DEBUG = 13;

    public static final int COM_PING = 14;

    public static final int COM_TIME = 15;

    public static final int COM_DELAYED_INSERT = 16;

    public static final int COM_CHANGE_USER = 17;

    public static final int COM_BINLOG_DUMP = 18;

    public static final int COM_TABLE_DUMP = 19;

    public static final int COM_CONNECT_OUT = 20;

    public static final int COM_REGISTER_SLAVE = 21;

    public static final int COM_STMT_PREPARE = 22;

    public static final int COM_STMT_EXECUTE = 23;

    public static final int COM_STMT_SEND_LONG_DATA = 24;

    public static final int COM_STMT_CLOSE = 25;

    public static final int COM_STMT_RESET = 26;

    public static final int COM_SET_OPTION = 27;

    public static final int COM_STMT_FETCH = 28;

    /** String equivalents of above commands for display purposes */
    static final String commandStrings[] = { "COM_SLEEP", "COM_QUIT", "COM_INIT_DB", "COM_QUERY", "COM_FIELD_LIST", "COM_CREATE_DB", "COM_DROP_DB", "COM_REFRESH", "COM_SHUTDOWN", "COM_STATISTICS",
            "COM_PROCESS_INFO", "COM_CONNECT", "COM_PROCESS_KILL", "COM_DEBUG", "COM_PING", "COM_TIME", "COM_DELAYED_INSERT", "COM_CHANGE_USER", "COM_BINLOG_DUMP", "COM_TABLE_DUMP",
            "COM_CONNECT_OUT", "COM_REGISTER_SLAVE", "COM_STMT_PREPARE", "COM_STMT_EXECUTE", "COM_STMT_SEND_LONG_DATA", "COM_STMT_CLOSE", "COM_STMT_RESET", "COM_SET_OPTION", "COM_STMT_FETCH" };

    /* Client flags from include/mysql_com.h. */
    public static final int CLIENT_LONG_PASSWORD = 1;

    public static final int CLIENT_FOUND_ROWS = 2;

    public static final int CLIENT_LONG_FLAG = 4;

    public static final int CLIENT_CONNECT_WITH_DB = 8;

    public static final int CLIENT_NO_SCHEMA = 16;

    public static final int CLIENT_COMPRESS = 32;

    public static final int CLIENT_ODBC = 64;

    public static final int CLIENT_LOCAL_FILES = 128;

    public static final int CLIENT_IGNORE_SPACE = 256;

    public static final int CLIENT_PROTOCOL_41 = 512;

    public static final int CLIENT_INTERACTIVE = 1024;

    public static final int CLIENT_SSL = 2048;

    public static final int CLIENT_IGNORE_SIGPIPE = 4096;

    public static final int CLIENT_TRANSACTIONS = 8192;

    public static final int CLIENT_RESERVED = 16384;

    public static final int CLIENT_SECURE_CONNECTION = 32768;

    public static final int CLIENT_MULTI_STATEMENTS = 65536;

    public static final int CLIENT_MULTI_RESULTS = 131072;

    /* Server status from include/mysql_com.h. */
    public static final short SERVER_STATUS_IN_TRANS = 1;

    public static final short SERVER_STATUS_AUTOCOMMIT = 2;

    public static final short SERVER_MORE_RESULTS_EXISTS = 8;

    public static final short SERVER_QUERY_NO_GOOD_INDEX_USED = 16;

    public static final short SERVER_QUERY_NO_INDEX_USED = 32;

    public static final int SERVER_STATUS_CURSOR_EXISTS = 64;

    public static final int SERVER_STATUS_LAST_ROW_SENT = 128;

    /* Field flags from include/mysql_com.h. */
    public static final int NOT_NULL_FLAG = 1;

    public static final int PRI_KEY_FLAG = 2;

    public static final int UNIQUE_KEY_FLAG = 4;

    public static final int MULTIPLE_KEY_FLAG = 8;

    public static final int BLOB_FLAG = 16;

    public static final int UNSIGNED_FLAG = 32;

    public static final int ZEROFILL_FLAG = 64;

    public static final int BINARY_FLAG = 128;

    public static final int ENUM_FLAG = 256;

    public static final int AUTO_INCREMENT_FLAG = 512;

    public static final int TIMESTAMP_FLAG = 1024;

    public static final int SET_FLAG = 2048;

    public static final int NO_DEFAULT_VALUE_FLAG = 4096;

    public static final int NUM_FLAG = 32768;

    /* Character set constants */
    public static final short CHARSET_BINARY = 63;

    public static final short CHARSET_UTF8 = 33;

    /* Cursor type */
    public static final byte CURSOR_TYPE_NO_CURSOR = (byte) 0;
    public static final byte CURSOR_TYPE_READ_ONLY = (byte) 1;
    public static final byte CURSOR_TYPE_FOR_UPDATE = (byte) 2;
    public static final byte CURSOR_TYPE_SCROLLABLE = (byte) 4;

    /* Type constants */
    /* These constants were extracted from include/mysql_com.h */

    public static final byte MYSQL_TYPE_DECIMAL = 0;

    public static final byte MYSQL_TYPE_TINY = 1;

    public static final byte MYSQL_TYPE_SHORT = 2;

    public static final byte MYSQL_TYPE_LONG = 3;

    public static final byte MYSQL_TYPE_FLOAT = 4;

    public static final byte MYSQL_TYPE_DOUBLE = 5;

    public static final byte MYSQL_TYPE_NULL = 6;

    public static final byte MYSQL_TYPE_TIMESTAMP = 7;

    public static final byte MYSQL_TYPE_LONGLONG = 8;

    public static final byte MYSQL_TYPE_INT24 = 9;

    public static final byte MYSQL_TYPE_DATE = 10;

    public static final byte MYSQL_TYPE_TIME = 11;

    public static final byte MYSQL_TYPE_DATETIME = 12;

    public static final byte MYSQL_TYPE_YEAR = 13;

    public static final byte MYSQL_TYPE_NEWDATE = 14;

    public static final byte MYSQL_TYPE_VARCHAR = 15;

    public static final byte MYSQL_TYPE_BIT = 16;

    public static final byte MYSQL_TYPE_NEWDECIMAL = -10;

    public static final byte MYSQL_TYPE_ENUM = -9;

    public static final byte MYSQL_TYPE_SET = -8;

    public static final byte MYSQL_TYPE_TINY_BLOB = -7;

    public static final byte MYSQL_TYPE_MEDIUM_BLOB = -6;

    public static final byte MYSQL_TYPE_LONG_BLOB = -5;

    public static final byte MYSQL_TYPE_BLOB = -4;

    public static final byte MYSQL_TYPE_VAR_STRING = -3;

    public static final byte MYSQL_TYPE_STRING = -2;

    public static final byte MYSQL_TYPE_GEOMETRY = -1;

    /* Errors taken from mysql_error.h */
    // ER_NO_ERROR is myosotis specific
    public static final int ER_NO_ERROR = -1;

    public static final int ER_NO = 1002;

    public static final int ER_DBACCESS_DENIED_ERROR = 1044;

    public static final int ER_BAD_DB_ERROR = 1049;

    public static final int ER_WRONG_ARGUMENTS = 1210;

    public static final int ER_NOT_SUPPORTED_YET = 1235;

    public static final int ER_UNKNOWN_STMT_HANDLER = 1243;

    public static final int ER_OUTOFMEMORY = 1037;

    public static final int ER_NO_DB_ERROR = 1046;

    public static final int ER_UNKNOWN_ERROR = 1055;

    public static final int ER_LOST_CONNECTION = 2013;

    public static final int ER_SERVER_GONE_AWAY = 2006;

    /* Misc constants */
    /** size of the long data header */
    public static final int MYSQL_LONG_DATA_HEADER = 6;

    public static final String SQL_STATE_COMMUNICATION_ERROR = "08S01";

    /** Private constructor to prevent instantiation. */
    private MySQLConstants() {
        // empty
    }

    /**
     * Converts the given command number into a human readable string
     *
     * @param command
     *            command number to convert, one of COM_XXX above
     * @return a string representing the given command constant name
     */
    public static String commandToString(int command) {
        return commandStrings[command];
    }
}
