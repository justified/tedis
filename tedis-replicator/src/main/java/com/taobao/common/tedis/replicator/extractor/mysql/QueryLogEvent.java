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
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.Enumeration;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.bytes.MySQLStatementTranslator;
import com.taobao.common.tedis.replicator.extractor.mysql.conversion.LittleEndianConversion;

public class QueryLogEvent extends LogEvent {
    static Logger logger = Logger.getLogger(MySQLExtractor.class);

    /**
     * Fixed data part:
     * <ul>
     * <li>4 bytes. The ID of the thread that issued this statement. Needed for
     * temporary tables. This is also useful for a DBA for knowing who did what
     * on the master.</li>
     * <li>4 bytes. The time in seconds that the statement took to execute. Only
     * useful for inspection by the DBA.</li>
     * <li>1 byte. The length of the name of the database which was the default
     * database when the statement was executed. This name appears later, in the
     * variable data part. It is necessary for statements such as INSERT INTO t
     * VALUES(1) that don't specify the database and rely on the default
     * database previously selected by USE.</li>
     * <li>2 bytes. The error code resulting from execution of the statement on
     * the master. Error codes are defined in include/mysqld_error.h. 0 means no
     * error. How come statements with a non-zero error code can exist in the
     * binary log? This is mainly due to the use of non-transactional tables
     * within transactions. For example, if an INSERT ... SELECT fails after
     * inserting 1000 rows into a MyISAM table (for example, with a
     * duplicate-key violation), we have to write this statement to the binary
     * log, because it truly modified the MyISAM table. For transactional
     * tables, there should be no event with a non-zero error code (though it
     * can happen, for example if the connection was interrupted (Control-C)).
     * The slave checks the error code: After executing the statement itself, it
     * compares the error code it got with the error code in the event, and if
     * they are different it stops replicating (unless --slave-skip-errors was
     * used to ignore the error).</li>
     * <li>2 bytes (not present in v1, v3). The length of the status variable
     * block.</li>
     * </ul>
     * Variable part:
     * <ul>
     * <li>Zero or more status variables (not present in v1, v3). Each status
     * variable consists of one byte code identifying the variable stored,
     * followed by the value of the variable. The format of the value is
     * variable-specific, as described later.</li>
     * <li>The default database name (null-terminated).</li>
     * <li>The SQL statement. The slave knows the size of the other fields in
     * the variable part (the sizes are given in the fixed data part), so by
     * subtraction it can know the size of the statement.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */

    protected String query;
    protected byte[] queryAsBytes;
    protected String charsetName;
    protected String databaseName;

    private int queryLength;
    protected int errorCode;
    protected long threadId;

    /*
     * Binlog format 3 and 4 start to differ (as far as class members are
     * concerned) from here.
     */
    private int catalogLength;

    protected boolean charset_inited;
    protected byte[] charset;
    protected int clientCharsetId;
    protected int clientCollationId;
    protected int serverCollationId;

    /*
     * 'flags2' is a second set of flags (on top of those in Log_event), for
     * session variables.
     */

    /* flags2 extracted variables */
    private boolean flagAutocommit = true;
    private boolean flagForeignKeyChecks = true;
    private boolean flagAutoIsNull = true;
    private boolean flagUniqueChecks = true;

    private long sql_mode;
    private String sqlModeAsString;

    // These fields are not used, so we stop extracting them for nothing
    // private long autoIncrementIncrement,
    // autoIncrementOffset;

    private int timeZoneLength;

    private String timeZoneName;

    // private int localeTimeNamesCode;
    // private int charset_database_number;
    // Store character set translator map.
    private static HashMap<String, MySQLStatementTranslator> translators = new HashMap<String, MySQLStatementTranslator>();
    protected boolean parseStatements;

    private String sessionVariables;

    private int autoIncrementIncrement = -1;

    private int autoIncrementOffset = -1;

    public String getQuery() {
        return query;
    }

    public String getDefaultDb() {
        return databaseName;
    }

    public long getSessionId() {
        return threadId;
    }

    public int getErrorCode() {
        return errorCode;
    }

    protected QueryLogEvent(byte[] buffer, FormatDescriptionLogEvent descriptionEvent, int eventType) throws ReplicatorException {
        super(buffer, descriptionEvent, eventType);
    }

    public QueryLogEvent(byte[] buffer, int eventLength, FormatDescriptionLogEvent descriptionEvent, boolean parseStatements, boolean useBytesForString) throws ReplicatorException {
        this(buffer, descriptionEvent, MysqlBinlog.QUERY_EVENT);
        this.parseStatements = parseStatements;

        int dataLength;
        int commonHeaderLength, postHeaderLength;
        int start;
        int end;
        boolean catalog_nz = true;
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

        short statusVariablesLength = 0;
        try {
            threadId = LittleEndianConversion.convert4BytesToLong(buffer, commonHeaderLength + MysqlBinlog.Q_THREAD_ID_OFFSET);

            execTime = LittleEndianConversion.convert4BytesToLong(buffer, commonHeaderLength + MysqlBinlog.Q_EXEC_TIME_OFFSET);

            databaseNameLength = LittleEndianConversion.convert1ByteToInt(buffer, commonHeaderLength + MysqlBinlog.Q_DB_LEN_OFFSET);

            // TODO: add a check of all *_len vars
            errorCode = LittleEndianConversion.convert2BytesToInt(buffer, commonHeaderLength + MysqlBinlog.Q_ERR_CODE_OFFSET);

            /*
             * 5.0 format starts here. Depending on the format, we may or not
             * have affected/warnings etc The remaining post-header to be parsed
             * has length:
             */
            boolean isMinimumMySQL5 = postHeaderLength - MysqlBinlog.QUERY_HEADER_MINIMAL_LEN > 0;

            if (isMinimumMySQL5) {
                statusVariablesLength = (short) LittleEndianConversion.convert2BytesToInt(buffer, commonHeaderLength + MysqlBinlog.Q_STATUS_VARS_LEN_OFFSET);

                dataLength -= statusVariablesLength;
                if (logger.isDebugEnabled())
                    logger.debug("QueryLogEvent has statusVariablesLength : " + statusVariablesLength);
            }
        } catch (IOException e) {
            throw new MySQLExtractException("query event header parsing failed");
        }

        /* variable part */

        /* Check the status variables if any */

        start = commonHeaderLength + postHeaderLength;
        end = start + statusVariablesLength;
        extractStatusVariables(buffer, start, end);

        if (catalogLength > 0) // If catalog is given
        {
            // true except if event comes from 5.0.0|1|2|3.
            if (catalog_nz == true) {
            } else {
            }
        }

        /* A 2nd variable part; this is common to all versions */
        databaseName = new String(buffer, end, databaseNameLength);
        queryLength = dataLength - databaseNameLength - 1;

        if (charset_inited) {
            try {
                // 6 byte character set flag:
                // 1-2 = character set client
                // 3-4 = collation client
                // 5-6 = collation server
                clientCharsetId = LittleEndianConversion.convert2BytesToInt(charset, 0);
                clientCollationId = LittleEndianConversion.convert2BytesToInt(charset, 2);
                serverCollationId = LittleEndianConversion.convert2BytesToInt(charset, 4);

                // Mark the query with the Java character set name.
                charsetName = MysqlBinlog.getJavaCharset(clientCharsetId);
            } catch (IOException e) {
                logger.error("failed to use character id: " + charset);
            }
        }

        if (useBytesForString) {
            queryAsBytes = new byte[queryLength];
            System.arraycopy(buffer, end + databaseNameLength + 1, queryAsBytes, 0, queryLength);
        } else if (charset_inited) {
            try {
                String DBcharset = MysqlBinlog.getJavaCharset(clientCharsetId);
                if (Charset.isSupported(DBcharset)) {
                    String charsetJava = MysqlBinlog.getJavaCharset(clientCharsetId);
                    if (logger.isDebugEnabled()) {
                        logger.debug("using charset: " + DBcharset + " java: " + charsetJava + " ID: " + clientCharsetId);
                    }
                    if (this.parseStatements) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Statement translation enabled");
                        }
                        MySQLStatementTranslator translator = getTranslator(DBcharset);
                        query = translator.toJavaString(buffer, end + databaseNameLength + 1, queryLength);
                    } else {
                        query = new String(buffer, end + databaseNameLength + 1, queryLength, DBcharset);
                    }
                } else {
                    logger.error("unsupported character set in query: " + DBcharset);
                    query = new String(buffer, end + databaseNameLength + 1, queryLength);
                }

            } catch (UnsupportedEncodingException e) {
                logger.error("failed to use character set: " + charset);
            } catch (IllegalCharsetNameException e) {
                if (logger.isDebugEnabled())
                    logger.debug("bad character set name: " + charset);
            } finally {
                if (query == null) {
                    logger.warn("Encoding query with default character set");
                    query = new String(buffer, end + databaseNameLength + 1, queryLength);
                }
            }
        } else {
            query = new String(buffer, end + databaseNameLength + 1, queryLength);
        }
    }

    protected int extractStatusVariables(byte[] buffer, int start, int end) throws ReplicatorException {
        int flags2;

        int pos;
        for (pos = start; pos < end;) {
            try {
                int variableCode = LittleEndianConversion.convert1ByteToInt(buffer, pos);
                pos++;
                switch (variableCode) {
                case MysqlBinlog.Q_FLAGS2_CODE:
                    /**
                     * 4 bytes bit-field : These flags correspond to the SQL
                     * variables SQL_AUTO_IS_NULL, FOREIGN_KEY_CHECKS,
                     * UNIQUE_CHECKS, and AUTOCOMMIT, documented in the "SET
                     * Syntax" section of the MySQL Manual. This field is always
                     * written to the binlog in version >= 5.0, and never
                     * written in version < 5.0.
                     */

                    flags2 = LittleEndianConversion.convert4BytesToInt(buffer, pos);
                    if (logger.isDebugEnabled())
                        logger.debug("In QueryLogEvent, flags2 = " + flags2 + " - row data : " + hexdump(buffer, pos, 4));

                    flagAutocommit = (flags2 & MysqlBinlog.OPTION_NOT_AUTOCOMMIT) != MysqlBinlog.OPTION_NOT_AUTOCOMMIT;
                    flagAutoIsNull = (flags2 & MysqlBinlog.OPTION_AUTO_IS_NULL) == MysqlBinlog.OPTION_AUTO_IS_NULL;
                    flagForeignKeyChecks = (flags2 & MysqlBinlog.OPTION_NO_FOREIGN_KEY_CHECKS) != MysqlBinlog.OPTION_NO_FOREIGN_KEY_CHECKS;
                    flagUniqueChecks = (flags2 & MysqlBinlog.OPTION_RELAXED_UNIQUE_CHECKS) != MysqlBinlog.OPTION_RELAXED_UNIQUE_CHECKS;

                    sessionVariables = "set @@session.foreign_key_checks=" + (flagForeignKeyChecks ? 1 : 0) + ", @@session.sql_auto_is_null=" + (flagAutoIsNull ? 1 : 0) + ", @@session.unique_checks="
                            + (flagUniqueChecks ? 1 : 0) + ", @@session.autocommit=" + (flagAutocommit ? 1 : 0);

                    if (logger.isDebugEnabled()) {
                        logger.debug(sessionVariables);
                    }
                    pos += 4;
                    break;
                case MysqlBinlog.Q_SQL_MODE_CODE: {
                    /**
                     * 8 byte bitfield
                     * <p>
                     * The sql_mode variable. See the section "SQL Modes" in the
                     * MySQL manual, and see mysql_priv.h for a list of the
                     * possible flags. Currently (2007-10-04), the following
                     * flags are available:
                     * <ul>
                     * <li>MODE_REAL_AS_FLOAT==0x1</li>
                     * <li>MODE_PIPES_AS_CONCAT==0x2</li>
                     * <li>MODE_ANSI_QUOTES==0x4</li>
                     * <li>MODE_IGNORE_SPACE==0x8</li>
                     * <li>MODE_NOT_USED==0x10</li>
                     * <li>MODE_ONLY_FULL_GROUP_BY==0x20</li>
                     * <li>MODE_NO_UNSIGNED_SUBTRACTION==0x40</li>
                     * <li>MODE_NO_DIR_IN_CREATE==0x80</li>
                     * <li>MODE_POSTGRESQL==0x100</li>
                     * <li>MODE_ORACLE==0x200</li>
                     * <li>MODE_MSSQL==0x400</li>
                     * <li>MODE_DB2==0x800</li>
                     * <li>MODE_MAXDB==0x1000</li>
                     * <li>MODE_NO_KEY_OPTIONS==0x2000</li>
                     * <li>MODE_NO_TABLE_OPTIONS==0x4000</li>
                     * <li>MODE_NO_FIELD_OPTIONS==0x8000</li>
                     * <li>MODE_MYSQL323==0x10000</li>
                     * <li>MODE_MYSQL40==0x20000</li>
                     * <li>MODE_ANSI==0x40000</li>
                     * <li>MODE_NO_AUTO_VALUE_ON_ZERO==0x80000</li>
                     * <li>MODE_NO_BACKSLASH_ESCAPES==0x100000</li>
                     * <li>MODE_STRICT_TRANS_TABLES==0x200000</li>
                     * <li>MODE_STRICT_ALL_TABLES==0x400000</li>
                     * <li>MODE_NO_ZERO_IN_DATE==0x800000</li>
                     * <li>MODE_NO_ZERO_DATE==0x1000000</li>
                     * <li>MODE_INVALID_DATES==0x2000000</li>
                     * <li>MODE_ERROR_FOR_DIVISION_BY_ZERO==0x4000000</li>
                     * <li>MODE_TRADITIONAL==0x8000000</li>
                     * <li>MODE_NO_AUTO_CREATE_USER==0x10000000</li>
                     * <li>MODE_HIGH_NOT_PRECEDENCE==0x20000000</li>
                     * <li>MODE_NO_ENGINE_SUBSTITUTION=0x40000000</li>
                     * <li>MODE_PAD_CHAR_TO_FULL_LENGTH==0x80000000</li>
                     * </ul>
                     * All these flags are replicated from the server. However,
                     * all flags except MODE_NO_DIR_IN_CREATE are honored by the
                     * slave; the slave always preserves its old value of
                     * MODE_NO_DIR_IN_CREATE. This field is always written to
                     * the binlog.
                     */
                    sql_mode = LittleEndianConversion.convert8BytesToLong(buffer, pos);
                    StringBuffer sqlMode = new StringBuffer("");
                    Enumeration<Long> keys = MysqlBinlog.sql_modes.keys();
                    while (keys.hasMoreElements()) {
                        Long mode = keys.nextElement();
                        if ((sql_mode & mode) == mode) {
                            if (sqlMode.length() > 0)
                                sqlMode.append(",");
                            sqlMode.append(MysqlBinlog.sql_modes.get(mode));
                        }
                    }
                    if (sql_mode != 0)
                        sqlModeAsString = "'" + sqlMode.toString() + "'";
                    else
                        sqlModeAsString = "''";

                    if (logger.isDebugEnabled())
                        logger.debug("In QueryLogEvent, sql_mode = " + sql_mode + (sql_mode != 0 ? " - " : "") + sqlModeAsString);
                    pos += 8;
                    break;
                }
                case MysqlBinlog.Q_CATALOG_NZ_CODE:
                    /**
                     * Variable-length string: the length in bytes (1 byte)
                     * followed by the characters (at most 255 bytes)
                     * <p>
                     * Stores the client's current catalog. Every database
                     * belongs to a catalog, the same way that every table
                     * belongs to a database. Currently, there is only one
                     * catalog, "std". This field is written if the length of
                     * the catalog is > 0; otherwise it is not written.
                     */
                    catalogLength = LittleEndianConversion.convert1ByteToInt(buffer, pos);
                    pos++;
                    if (catalogLength > 0) {
                        // Catalog name is String(buf, common_header_len +
                        // pos, catalog_len), but we don't use it...
                        pos += catalogLength;
                    }
                    break;
                case MysqlBinlog.Q_AUTO_INCREMENT:
                    /**
                     * two 2 byte unsigned integers, totally 2+2=4 bytes
                     * <p>
                     * The two variables auto_increment_increment and
                     * auto_increment_offset, in that order. For more
                     * information, see "System variables" in the MySQL manual.
                     * This field is written if auto_increment > 1. Otherwise,
                     * it is not written.
                     */
                    autoIncrementIncrement = LittleEndianConversion.convert2BytesToInt(buffer, pos);
                    pos += 2;
                    autoIncrementOffset = LittleEndianConversion.convert2BytesToInt(buffer, pos);
                    pos += 2;
                    break;
                case MysqlBinlog.Q_CHARSET_CODE: {
                    charset_inited = true;
                    charset = new byte[6];
                    System.arraycopy(buffer, pos, charset, 0, 6);
                    pos += 6;
                    break;
                }
                case MysqlBinlog.Q_TIME_ZONE_CODE: {
                    /**
                     * Variable-length string: the length in bytes (1 byte)
                     * followed by the characters (at most 255 bytes).
                     * <p>
                     * The time_zone of the master. This field is written if the
                     * length of the time zone string is > 0; otherwise, it is
                     * not written.
                     */
                    timeZoneLength = LittleEndianConversion.convert1ByteToInt(buffer, pos);
                    pos++;
                    if (timeZoneLength > 0) {
                        timeZoneName = new String(buffer, pos, timeZoneLength);
                        if (logger.isDebugEnabled())
                            logger.debug("Using time zone : " + timeZoneName);
                        pos += timeZoneLength;
                    }
                    break;
                }
                case MysqlBinlog.Q_CATALOG_CODE:
                    /*
                     * TODO for 5.0.x where 0<=x<=3 masters ==> Unsupported ?
                     */
                    break;
                case MysqlBinlog.Q_LC_TIME_NAMES_CODE:
                    /**
                     * 2 byte integer
                     * <p>
                     * A code identifying a table of month and day names. This
                     * field is written if it is not 0, i.e., if the locale is
                     * not en_US.
                     */
                    // Stopping extracting unused fields
                    // localeTimeNamesCode = LittleEndianConversion
                    // .convert2BytesToInt_2(buffer,
                    // commonHeaderLength + pos);
                    pos += 2;
                    break;
                case MysqlBinlog.Q_CHARSET_DATABASE_CODE:
                    // Stopping extracting unused fields
                    // charset_database_number = LittleEndianConversion
                    // .convert2BytesToInt_2(buffer,
                    // commonHeaderLength + pos);
                    pos += 2;
                    break;
                default:
                    if (logger.isDebugEnabled())
                        logger.debug("QueryLogEvent has unknown status variable +" + "(first has code: " + (pos + 1) + " ), skipping the rest of them");
                    pos = end;
                }
            } catch (IOException e) {
                throw new MySQLExtractException("IO exception while reading query event parameters");
            }
        }
        return pos;
    }

    // Fetch a character set translator. Create one if necessary.
    private MySQLStatementTranslator getTranslator(String charset) throws UnsupportedEncodingException {
        MySQLStatementTranslator translator = translators.get(charset);
        if (translator == null) {
            translator = new MySQLStatementTranslator(charset);
            translators.put(charset, translator);
        }
        return translator;
    }

    /**
     * Returns the sqlModeAsString value.
     *
     * @return Returns the sqlModeAsString.
     */
    public String getSqlMode() {
        return sqlModeAsString;
    }

    /**
     * Returns the flagAutoIsNull value.
     *
     * @return Returns the flagAutoIsNull.
     */
    public String getAutoIsNullFlag() {
        return (flagAutoIsNull ? "1" : "0");
    }

    /**
     * Returns the flagForeignKeyChecks value.
     *
     * @return Returns the flagForeignKeyChecks.
     */
    public String getForeignKeyChecksFlag() {
        return (flagForeignKeyChecks ? "1" : "0");
    }

    /**
     * Returns the flagAutocommit value.
     *
     * @return Returns the flagAutocommit.
     */
    public String getAutocommitFlag() {
        return (flagAutocommit ? "1" : "0");
    }

    /**
     * Returns the flagUniqueChecks value.
     *
     * @return Returns the flagUniqueChecks.
     */
    public String getUniqueChecksFlag() {
        return (flagUniqueChecks ? "1" : "0");
    }

    /**
     * Returns the charsetID value.
     *
     * @return Returns the charsetID.
     */
    public int getClientCharsetId() {
        return clientCharsetId;
    }

    /**
     * Returns the clientCollationId value.
     *
     * @return Returns the clientCollationId.
     */
    public int getClientCollationId() {
        return clientCollationId;
    }

    /**
     * Returns the serverCollationId value.
     *
     * @return Returns the serverCollationId.
     */
    public int getServerCollationId() {
        return serverCollationId;
    }

    public int getAutoIncrementIncrement() {
        return autoIncrementIncrement;
    }

    public int getAutoIncrementOffset() {
        return autoIncrementOffset;
    }

    public byte[] getQueryAsBytes() {
        return queryAsBytes;
    }

    /** Return the native charset used to store query as bytes. */
    public String getCharsetName() {
        return charsetName;
    }
}
