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
import java.util.BitSet;
import java.util.Hashtable;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.extractor.mysql.conversion.BigEndianConversion;
import com.taobao.common.tedis.replicator.extractor.mysql.conversion.LittleEndianConversion;

public class MysqlBinlog {

    static Logger logger = Logger.getLogger(MysqlBinlog.class);

    public static final int EVENT_TYPE_OFFSET = 4;
    public static final int SERVER_ID_OFFSET = 5;
    public static final int EVENT_LEN_OFFSET = 9;
    public static final int LOG_POS_OFFSET = 13;
    public static final int FLAGS_OFFSET = 17;
    public static final int LOG_EVENT_THREAD_SPECIFIC_F = 0x4;

    public static final int BIN_LOG_HEADER_SIZE = 4;
    public static final int PROBE_HEADER_LEN = EVENT_LEN_OFFSET + 4;

    public static final byte[] BINLOG_MAGIC = { (byte) 0xfe, 0x62, 0x69, 0x6e };

    // Binlog versions.
    public static final int VERSION_NONE = 0;
    public static final int BINLOG_V1 = 1;
    public static final int BINLOG_V3 = 2;
    public static final int BINLOG_V4 = 3;

    /* binlog event types */
    public static final int UNKNOWN_EVENT = 0;
    public static final int START_EVENT_V3 = 1;
    public static final int QUERY_EVENT = 2;
    public static final int STOP_EVENT = 3;
    public static final int ROTATE_EVENT = 4;
    public static final int INTVAR_EVENT = 5;
    public static final int LOAD_EVENT = 6;
    public static final int SLAVE_EVENT = 7;
    public static final int CREATE_FILE_EVENT = 8;
    public static final int APPEND_BLOCK_EVENT = 9;
    public static final int EXEC_LOAD_EVENT = 10;
    public static final int DELETE_FILE_EVENT = 11;
    public static final int NEW_LOAD_EVENT = 12;
    public static final int RAND_EVENT = 13;
    public static final int USER_VAR_EVENT = 14;
    public static final int FORMAT_DESCRIPTION_EVENT = 15;
    public static final int XID_EVENT = 16;
    public static final int BEGIN_LOAD_QUERY_EVENT = 17;
    public static final int EXECUTE_LOAD_QUERY_EVENT = 18;

    public static final int TABLE_MAP_EVENT = 19;

    /*
     * These event numbers were used for 5.1.0 to 5.1.15 and are therefore
     * obsolete.
     */
    public static final int PRE_GA_WRITE_ROWS_EVENT = 20;
    public static final int PRE_GA_UPDATE_ROWS_EVENT = 21;
    public static final int PRE_GA_DELETE_ROWS_EVENT = 22;

    /*
     * These event numbers are used from 5.1.16 and forward
     */
    public static final int WRITE_ROWS_EVENT = 23;
    public static final int UPDATE_ROWS_EVENT = 24;
    public static final int DELETE_ROWS_EVENT = 25;

    /*
     * Something out of the ordinary happened on the master
     */
    public static final int INCIDENT_EVENT = 26;

    /*
     * Add new events here - right above this comment! Existing events (except
     * ENUM_END_EVENT) should never change their numbers
     */

    public static final int ENUM_END_EVENT = 27;
    /*
     * end marker
     */

    public static final int ST_SERVER_VER_LEN = 50;
    public static final int LOG_EVENT_TYPES = 26;

    public static final int OLD_HEADER_LEN = 13;
    public static final int LOG_EVENT_HEADER_LEN = 19;
    public static final int LOG_EVENT_MINIMAL_HEADER_LEN = 19;

    /* event-specific post-header sizes */
    // where 3.23, 4.x and 5.0 agree
    public static final int QUERY_HEADER_MINIMAL_LEN = (4 + 4 + 1 + 2);
    // where 5.0 differs: 2 for len of N-bytes vars.
    public static final int QUERY_HEADER_LEN = (QUERY_HEADER_MINIMAL_LEN + 2);
    public static final int LOAD_HEADER_LEN = (4 + 4 + 4 + 1 + 1 + 4);
    public static final int START_V3_HEADER_LEN = (2 + ST_SERVER_VER_LEN + 4);
    public static final int ROTATE_HEADER_LEN = 8;
    // this is FROZEN (the Rotate post-header is frozen)
    public static final int CREATE_FILE_HEADER_LEN = 4;
    public static final int APPEND_BLOCK_HEADER_LEN = 4;
    public static final int EXEC_LOAD_HEADER_LEN = 4;
    public static final int DELETE_FILE_HEADER_LEN = 4;
    public static final int FORMAT_DESCRIPTION_HEADER_LEN = (START_V3_HEADER_LEN + 1 + LOG_EVENT_TYPES);
    public static final int ROWS_HEADER_LEN = 8;
    public static final int TABLE_MAP_HEADER_LEN = 8;
    public static final int EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN = (4 + 4 + 4 + 1);
    public static final int EXECUTE_LOAD_QUERY_HEADER_LEN = (QUERY_HEADER_LEN + EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN);
    public static final int INCIDENT_HEADER_LEN = 2;

    /* start event post-header (for v3 and v4) */
    public static final int ST_BINLOG_VER_OFFSET = 0;
    public static final int ST_SERVER_VER_OFFSET = 2;
    public static final int ST_CREATED_OFFSET = (ST_SERVER_VER_OFFSET + ST_SERVER_VER_LEN);
    public static final int ST_COMMON_HEADER_LEN_OFFSET = (ST_CREATED_OFFSET + 4);

    /* slave event post-header (this event is never written) */
    public static final int SL_MASTER_PORT_OFFSET = 8;
    public static final int SL_MASTER_POS_OFFSET = 0;
    public static final int SL_MASTER_HOST_OFFSET = 10;

    /* query log event constants */
    public static final int Q_THREAD_ID_OFFSET = 0;
    public static final int Q_EXEC_TIME_OFFSET = 4;
    public static final int Q_DB_LEN_OFFSET = 8;
    public static final int Q_ERR_CODE_OFFSET = 9;
    public static final int Q_STATUS_VARS_LEN_OFFSET = 11;
    public static final int Q_DATA_OFFSET = QUERY_HEADER_LEN;
    /* these are codes, not offsets; not more than 256 values (1 byte). */
    public static final int Q_FLAGS2_CODE = 0;
    public static final int Q_SQL_MODE_CODE = 1;

    /* FLAGS2 values that can be represented inside the binlog */
    public static final int OPTION_AUTO_IS_NULL = 1 << 14;
    public static final int OPTION_NOT_AUTOCOMMIT = 1 << 19;
    public static final int OPTION_NO_FOREIGN_KEY_CHECKS = 1 << 26;
    public static final int OPTION_RELAXED_UNIQUE_CHECKS = 1 << 27;

    public static final Hashtable<Long, String> sql_modes = new Hashtable<Long, String>();

    /* SQL_MODE */
    static {
        sql_modes.put(Long.valueOf(0x1), "REAL_AS_FLOAT");
        sql_modes.put(Long.valueOf(0x2), "PIPES_AS_CONCAT");
        sql_modes.put(Long.valueOf(0x4), "ANSI_QUOTES");
        sql_modes.put(Long.valueOf(0x8), "IGNORE_SPACE");
        sql_modes.put(Long.valueOf(0x10), "NOT_USED");
        sql_modes.put(Long.valueOf(0x20), "ONLY_FULL_GROUP_BY");
        sql_modes.put(Long.valueOf(0x40), "NO_UNSIGNED_SUBTRACTION");
        sql_modes.put(Long.valueOf(0x80), "NO_DIR_IN_CREATE");
        sql_modes.put(Long.valueOf(0x100), "POSTGRESQL");
        sql_modes.put(Long.valueOf(0x200), "ORACLE");
        sql_modes.put(Long.valueOf(0x400), "MSSQL");
        sql_modes.put(Long.valueOf(0x800), "DB2");
        sql_modes.put(Long.valueOf(0x1000), "MAXDB");
        sql_modes.put(Long.valueOf(0x2000), "NO_KEY_OPTIONS");
        sql_modes.put(Long.valueOf(0x4000), "NO_TABLE_OPTIONS");
        sql_modes.put(Long.valueOf(0x8000), "NO_FIELD_OPTIONS");
        sql_modes.put(Long.valueOf(0x10000), "MYSQL323");
        sql_modes.put(Long.valueOf(0x20000), "MYSQL40");
        sql_modes.put(Long.valueOf(0x40000), "ANSI");
        sql_modes.put(Long.valueOf(0x80000), "NO_AUTO_VALUE_ON_ZERO");
        sql_modes.put(Long.valueOf(0x100000), "NO_BACKSLASH_ESCAPES");
        sql_modes.put(Long.valueOf(0x200000), "STRICT_TRANS_TABLES");
        sql_modes.put(Long.valueOf(0x400000), "STRICT_ALL_TABLES");
        sql_modes.put(Long.valueOf(0x800000), "NO_ZERO_IN_DATE");
        sql_modes.put(Long.valueOf(0x1000000), "NO_ZERO_DATE");
        sql_modes.put(Long.valueOf(0x2000000), "INVALID_DATES");
        sql_modes.put(Long.valueOf(0x4000000), "ERROR_FOR_DIVISION_BY_ZERO");
        sql_modes.put(Long.valueOf(0x8000000), "TRADITIONAL");
        sql_modes.put(Long.valueOf(0x10000000), "NO_AUTO_CREATE_USER");
        sql_modes.put(Long.valueOf(0x20000000), "HIGH_NOT_PRECEDENCE");
        sql_modes.put(Long.valueOf(0x40000000), "NO_ENGINE_SUBSTITUTION");
        sql_modes.put(Long.valueOf(0x80000000), "PAD_CHAR_TO_FULL_LENGTH");
    }

    /*
     * Q_CATALOG_CODE is catalog with end zero stored; it is used only by MySQL
     * 5.0.x where 0<=x<=3.
     */
    public static final int Q_CATALOG_CODE = 2;
    public static final int Q_AUTO_INCREMENT = 3;
    public static final int Q_CHARSET_CODE = 4;
    public static final int Q_TIME_ZONE_CODE = 5;
    /*
     * Q_CATALOG_NZ_CODE is catalog withOUT end zero stored; it is used by MySQL
     * 5.0.x where x>=4.
     */
    public static final int Q_CATALOG_NZ_CODE = 6;
    public static final int Q_LC_TIME_NAMES_CODE = 7;
    public static final int Q_CHARSET_DATABASE_CODE = 8;

    /* Intvar event post-header */
    public static final int I_TYPE_OFFSET = 0;
    public static final int I_VAL_OFFSET = 1;

    /* Rand event post-header */
    public static final int RAND_SEED1_OFFSET = 0;
    public static final int RAND_SEED2_OFFSET = 8;

    /* User_var event post-header */
    public static final int UV_VAL_LEN_SIZE = 4;
    public static final int UV_VAL_IS_NULL = 1;
    public static final int UV_VAL_TYPE_SIZE = 1;
    public static final int UV_NAME_LEN_SIZE = 4;
    public static final int UV_CHARSET_NUMBER_SIZE = 4;

    /* Rotate log events */
    public static final int R_POS_OFFSET = 0;
    public static final int R_IDENT_OFFSET = 8;

    /* Table map event */
    public static final int TM_MAPID_OFFSET = 0;
    public static final int TM_FLAGS_OFFSET = 6;

    /* rows log event */
    public static final int RW_MAPID_OFFSET = 0;
    public static final int RW_FLAGS_OFFSET = 6;

    public static final int FN_REFLEN = 512;
    public static final long LONG_MAX = 0x7FFFFFFFL;
    public static final long NULL_LENGTH = LONG_MAX;

    public static final int MYSQL_TYPE_DECIMAL = 0;
    public static final int MYSQL_TYPE_TINY = 1;
    public static final int MYSQL_TYPE_SHORT = 2;
    public static final int MYSQL_TYPE_LONG = 3;
    public static final int MYSQL_TYPE_FLOAT = 4;
    public static final int MYSQL_TYPE_DOUBLE = 5;
    public static final int MYSQL_TYPE_NULL = 6;
    public static final int MYSQL_TYPE_TIMESTAMP = 7;
    public static final int MYSQL_TYPE_LONGLONG = 8;
    public static final int MYSQL_TYPE_INT24 = 9;
    public static final int MYSQL_TYPE_DATE = 10;
    public static final int MYSQL_TYPE_TIME = 11;
    public static final int MYSQL_TYPE_DATETIME = 12;
    public static final int MYSQL_TYPE_YEAR = 13;
    public static final int MYSQL_TYPE_NEWDATE = 14;
    public static final int MYSQL_TYPE_VARCHAR = 15;
    public static final int MYSQL_TYPE_BIT = 16;
    public static final int MYSQL_TYPE_NEWDECIMAL = 246;
    public static final int MYSQL_TYPE_ENUM = 247;
    public static final int MYSQL_TYPE_SET = 248;
    public static final int MYSQL_TYPE_TINY_BLOB = 249;
    public static final int MYSQL_TYPE_MEDIUM_BLOB = 250;
    public static final int MYSQL_TYPE_LONG_BLOB = 251;
    public static final int MYSQL_TYPE_BLOB = 252;
    public static final int MYSQL_TYPE_VAR_STRING = 253;
    public static final int MYSQL_TYPE_STRING = 254;
    public static final int MYSQL_TYPE_GEOMETRY = 255;

    public static final int TINYINT_MIN = -128;
    public static final int TINYINT_MAX = 127;
    public static final int SMALLINT_MIN = -32768;
    public static final int SMALLINT_MAX = 32767;
    public static final int MEDIUMINT_MIN = -8388608;
    public static final int MEDIUMINT_MAX = 8388607;
    public static final int INT_MIN = -2147483648;
    public static final int INT_MAX = 2147483647;

    /* decimal representation */
    public static int DIG_PER_DEC1 = 9;
    public static int DIG_BASE = 1000000000;
    public static int DIG_MAX = DIG_BASE - 1;
    public static final int dig2bytes[] = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };

    public static final int E_DEC_OVERFLOW = 100;
    public static final int E_DEC_TRUNCATED = 101;

    public final static int DIG_PER_INT32 = 9;
    public final static int SIZE_OF_INT32 = 4;

    // Class to define character set information.
    static class CharsetInfo {
        final int index;
        final String mysqlCharset;
        final String mysqlCollation;
        final String javaCharset;

        CharsetInfo(int index, String mysqlCharset, String mysqlCollation, String javaCharset) {
            this.index = index;
            this.mysqlCharset = mysqlCharset;
            this.mysqlCollation = mysqlCollation;
            this.javaCharset = javaCharset;
        }
    }

    // Character set data used in lookups. The array will be sparse.
    public static CharsetInfo[] charsets = new CharsetInfo[255];

    // Load character set data statically.
    static {
        loadCharset(1, "big5", "big5_chinese_ci");
        loadCharset(2, "latin2", "latin2_czech_cs");
        loadCharset(3, "dec8", "dec8_swedish_ci");
        loadCharset(4, "cp850", "cp850_general_ci");
        loadCharset(5, "latin1", "latin1_german1_ci");
        loadCharset(6, "hp8", "hp8_english_ci");
        loadCharset(7, "koi8r", "koi8r_general_ci");
        loadCharset(8, "latin1", "latin1_swedish_ci");
        loadCharset(9, "latin2", "latin2_general_ci");
        loadCharset(10, "swe7", "swe7_swedish_ci");
        loadCharset(11, "ascii", "ascii_general_ci");
        loadCharset(12, "ujis", "ujis_japanese_ci");
        loadCharset(13, "sjis", "sjis_japanese_ci");
        loadCharset(14, "cp1251", "cp1251_bulgarian_ci");
        loadCharset(15, "latin1", "latin1_danish_ci");
        loadCharset(16, "hebrew", "hebrew_general_ci");
        loadCharset(18, "tis620", "tis620_thai_ci");
        loadCharset(19, "euckr", "euckr_korean_ci");
        loadCharset(20, "latin7", "latin7_estonian_cs");
        loadCharset(21, "latin2", "latin2_hungarian_ci");
        loadCharset(22, "koi8u", "koi8u_general_ci");
        loadCharset(23, "cp1251", "cp1251_ukrainian_ci");
        loadCharset(24, "gb2312", "gb2312_chinese_ci");
        loadCharset(25, "greek", "greek_general_ci");
        loadCharset(26, "cp1250", "cp1250_general_ci");
        loadCharset(27, "latin2", "latin2_croatian_ci");
        loadCharset(28, "gbk", "gbk_chinese_ci");
        loadCharset(29, "cp1257", "cp1257_lithuanian_ci");
        loadCharset(30, "latin5", "latin5_turkish_ci");
        loadCharset(31, "latin1", "latin1_german2_ci");
        loadCharset(32, "armscii8", "armscii8_general_ci");
        loadCharset(33, "utf8", "utf8_general_ci");
        loadCharset(34, "cp1250", "cp1250_czech_cs");
        loadCharset(35, "ucs2", "ucs2_general_ci");
        loadCharset(36, "cp866", "cp866_general_ci");
        loadCharset(37, "keybcs2", "keybcs2_general_ci");
        loadCharset(38, "macce", "macce_general_ci");
        loadCharset(39, "macroman", "macroman_general_ci");
        loadCharset(40, "cp852", "cp852_general_ci");
        loadCharset(41, "latin7", "latin7_general_ci");
        loadCharset(42, "latin7", "latin7_general_cs");
        loadCharset(43, "macce", "macce_bin");
        loadCharset(44, "cp1250", "cp1250_croatian_ci");
        loadCharset(45, "utf8mb4", "utf8mb4_general_ci");
        loadCharset(46, "utf8mb4", "utf8mb4_bin");
        loadCharset(47, "latin1", "latin1_bin");
        loadCharset(48, "latin1", "latin1_general_ci");
        loadCharset(49, "latin1", "latin1_general_cs");
        loadCharset(50, "cp1251", "cp1251_bin");
        loadCharset(51, "cp1251", "cp1251_general_ci");
        loadCharset(52, "cp1251", "cp1251_general_cs");
        loadCharset(53, "macroman", "macroman_bin");
        loadCharset(54, "utf16", "utf16_general_ci");
        loadCharset(55, "utf16", "utf16_bin");
        loadCharset(57, "cp1256", "cp1256_general_ci");
        loadCharset(58, "cp1257", "cp1257_bin");
        loadCharset(59, "cp1257", "cp1257_general_ci");
        loadCharset(60, "utf32", "utf32_general_ci");
        loadCharset(61, "utf32", "utf32_bin");
        loadCharset(63, "binary", "binary");
        loadCharset(64, "armscii8", "armscii8_bin");
        loadCharset(65, "ascii", "ascii_bin");
        loadCharset(66, "cp1250", "cp1250_bin");
        loadCharset(67, "cp1256", "cp1256_bin");
        loadCharset(68, "cp866", "cp866_bin");
        loadCharset(69, "dec8", "dec8_bin");
        loadCharset(70, "greek", "greek_bin");
        loadCharset(71, "hebrew", "hebrew_bin");
        loadCharset(72, "hp8", "hp8_bin");
        loadCharset(73, "keybcs2", "keybcs2_bin");
        loadCharset(74, "koi8r", "koi8r_bin");
        loadCharset(75, "koi8u", "koi8u_bin");
        loadCharset(77, "latin2", "latin2_bin");
        loadCharset(78, "latin5", "latin5_bin");
        loadCharset(79, "latin7", "latin7_bin");
        loadCharset(80, "cp850", "cp850_bin");
        loadCharset(81, "cp852", "cp852_bin");
        loadCharset(82, "swe7", "swe7_bin");
        loadCharset(83, "utf8", "utf8_bin");
        loadCharset(84, "big5", "big5_bin");
        loadCharset(85, "euckr", "euckr_bin");
        loadCharset(86, "gb2312", "gb2312_bin");
        loadCharset(87, "gbk", "gbk_bin");
        loadCharset(88, "sjis", "sjis_bin");
        loadCharset(89, "tis620", "tis620_bin");
        loadCharset(90, "ucs2", "ucs2_bin");
        loadCharset(91, "ujis", "ujis_bin");
        loadCharset(92, "geostd8", "geostd8_general_ci");
        loadCharset(93, "geostd8", "geostd8_bin");
        loadCharset(94, "latin1", "latin1_spanish_ci");
        loadCharset(95, "cp932", "cp932_japanese_ci");
        loadCharset(96, "cp932", "cp932_bin");
        loadCharset(97, "eucjpms", "eucjpms_japanese_ci");
        loadCharset(98, "eucjpms", "eucjpms_bin");
        loadCharset(99, "cp1250", "cp1250_polish_ci");
        loadCharset(101, "utf16", "utf16_unicode_ci");
        loadCharset(102, "utf16", "utf16_icelandic_ci");
        loadCharset(103, "utf16", "utf16_latvian_ci");
        loadCharset(104, "utf16", "utf16_romanian_ci");
        loadCharset(105, "utf16", "utf16_slovenian_ci");
        loadCharset(106, "utf16", "utf16_polish_ci");
        loadCharset(107, "utf16", "utf16_estonian_ci");
        loadCharset(108, "utf16", "utf16_spanish_ci");
        loadCharset(109, "utf16", "utf16_swedish_ci");
        loadCharset(110, "utf16", "utf16_turkish_ci");
        loadCharset(111, "utf16", "utf16_czech_ci");
        loadCharset(112, "utf16", "utf16_danish_ci");
        loadCharset(113, "utf16", "utf16_lithuanian_ci");
        loadCharset(114, "utf16", "utf16_slovak_ci");
        loadCharset(115, "utf16", "utf16_spanish2_ci");
        loadCharset(116, "utf16", "utf16_roman_ci");
        loadCharset(117, "utf16", "utf16_persian_ci");
        loadCharset(118, "utf16", "utf16_esperanto_ci");
        loadCharset(119, "utf16", "utf16_hungarian_ci");
        loadCharset(120, "utf16", "utf16_sinhala_ci");
        loadCharset(128, "ucs2", "ucs2_unicode_ci");
        loadCharset(129, "ucs2", "ucs2_icelandic_ci");
        loadCharset(130, "ucs2", "ucs2_latvian_ci");
        loadCharset(131, "ucs2", "ucs2_romanian_ci");
        loadCharset(132, "ucs2", "ucs2_slovenian_ci");
        loadCharset(133, "ucs2", "ucs2_polish_ci");
        loadCharset(134, "ucs2", "ucs2_estonian_ci");
        loadCharset(135, "ucs2", "ucs2_spanish_ci");
        loadCharset(136, "ucs2", "ucs2_swedish_ci");
        loadCharset(137, "ucs2", "ucs2_turkish_ci");
        loadCharset(138, "ucs2", "ucs2_czech_ci");
        loadCharset(139, "ucs2", "ucs2_danish_ci");
        loadCharset(140, "ucs2", "ucs2_lithuanian_ci");
        loadCharset(141, "ucs2", "ucs2_slovak_ci");
        loadCharset(142, "ucs2", "ucs2_spanish2_ci");
        loadCharset(143, "ucs2", "ucs2_roman_ci");
        loadCharset(144, "ucs2", "ucs2_persian_ci");
        loadCharset(145, "ucs2", "ucs2_esperanto_ci");
        loadCharset(146, "ucs2", "ucs2_hungarian_ci");
        loadCharset(147, "ucs2", "ucs2_sinhala_ci");
        loadCharset(160, "utf32", "utf32_unicode_ci");
        loadCharset(161, "utf32", "utf32_icelandic_ci");
        loadCharset(162, "utf32", "utf32_latvian_ci");
        loadCharset(163, "utf32", "utf32_romanian_ci");
        loadCharset(164, "utf32", "utf32_slovenian_ci");
        loadCharset(165, "utf32", "utf32_polish_ci");
        loadCharset(166, "utf32", "utf32_estonian_ci");
        loadCharset(167, "utf32", "utf32_spanish_ci");
        loadCharset(168, "utf32", "utf32_swedish_ci");
        loadCharset(169, "utf32", "utf32_turkish_ci");
        loadCharset(170, "utf32", "utf32_czech_ci");
        loadCharset(171, "utf32", "utf32_danish_ci");
        loadCharset(172, "utf32", "utf32_lithuanian_ci");
        loadCharset(173, "utf32", "utf32_slovak_ci");
        loadCharset(174, "utf32", "utf32_spanish2_ci");
        loadCharset(175, "utf32", "utf32_roman_ci");
        loadCharset(176, "utf32", "utf32_persian_ci");
        loadCharset(177, "utf32", "utf32_esperanto_ci");
        loadCharset(178, "utf32", "utf32_hungarian_ci");
        loadCharset(179, "utf32", "utf32_sinhala_ci");
        loadCharset(192, "utf8", "utf8_unicode_ci");
        loadCharset(193, "utf8", "utf8_icelandic_ci");
        loadCharset(194, "utf8", "utf8_latvian_ci");
        loadCharset(195, "utf8", "utf8_romanian_ci");
        loadCharset(196, "utf8", "utf8_slovenian_ci");
        loadCharset(197, "utf8", "utf8_polish_ci");
        loadCharset(198, "utf8", "utf8_estonian_ci");
        loadCharset(199, "utf8", "utf8_spanish_ci");
        loadCharset(200, "utf8", "utf8_swedish_ci");
        loadCharset(201, "utf8", "utf8_turkish_ci");
        loadCharset(202, "utf8", "utf8_czech_ci");
        loadCharset(203, "utf8", "utf8_danish_ci");
        loadCharset(204, "utf8", "utf8_lithuanian_ci");
        loadCharset(205, "utf8", "utf8_slovak_ci");
        loadCharset(206, "utf8", "utf8_spanish2_ci");
        loadCharset(207, "utf8", "utf8_roman_ci");
        loadCharset(208, "utf8", "utf8_persian_ci");
        loadCharset(209, "utf8", "utf8_esperanto_ci");
        loadCharset(210, "utf8", "utf8_hungarian_ci");
        loadCharset(211, "utf8", "utf8_sinhala_ci");
        loadCharset(224, "utf8mb4", "utf8mb4_unicode_ci");
        loadCharset(225, "utf8mb4", "utf8mb4_icelandic_ci");
        loadCharset(226, "utf8mb4", "utf8mb4_latvian_ci");
        loadCharset(227, "utf8mb4", "utf8mb4_romanian_ci");
        loadCharset(228, "utf8mb4", "utf8mb4_slovenian_ci");
        loadCharset(229, "utf8mb4", "utf8mb4_polish_ci");
        loadCharset(230, "utf8mb4", "utf8mb4_estonian_ci");
        loadCharset(231, "utf8mb4", "utf8mb4_spanish_ci");
        loadCharset(232, "utf8mb4", "utf8mb4_swedish_ci");
        loadCharset(233, "utf8mb4", "utf8mb4_turkish_ci");
        loadCharset(234, "utf8mb4", "utf8mb4_czech_ci");
        loadCharset(235, "utf8mb4", "utf8mb4_danish_ci");
        loadCharset(236, "utf8mb4", "utf8mb4_lithuanian_ci");
        loadCharset(237, "utf8mb4", "utf8mb4_slovak_ci");
        loadCharset(238, "utf8mb4", "utf8mb4_spanish2_ci");
        loadCharset(239, "utf8mb4", "utf8mb4_roman_ci");
        loadCharset(240, "utf8mb4", "utf8mb4_persian_ci");
        loadCharset(241, "utf8mb4", "utf8mb4_esperanto_ci");
        loadCharset(242, "utf8mb4", "utf8mb4_hungarian_ci");
        loadCharset(243, "utf8mb4", "utf8mb4_sinhala_ci");
        loadCharset(254, "utf8", "utf8_general_cs");
    }

    // Loads character set information.
    private static void loadCharset(int index, String mysqlCharset, String mysqlCollation) {
        String javaCharset = null;
        if ("armscii8".equals(mysqlCharset))
            javaCharset = "";
        else if ("ascii".equals(mysqlCharset))
            javaCharset = "US-ASCII";
        else if ("big5".equals(mysqlCharset))
            javaCharset = "Big5";
        else if ("binary".equals(mysqlCharset))
            javaCharset = "";
        else if ("cp1250".equals(mysqlCharset))
            javaCharset = "Cp1250";
        else if ("cp1251".equals(mysqlCharset))
            javaCharset = "Cp1251";
        else if ("cp1256".equals(mysqlCharset))
            javaCharset = "";
        else if ("cp1257".equals(mysqlCharset))
            javaCharset = "Cp1257";
        else if ("cp850".equals(mysqlCharset))
            javaCharset = "cp850";
        else if ("cp852".equals(mysqlCharset))
            javaCharset = "";
        else if ("cp866".equals(mysqlCharset))
            javaCharset = "Cp866";
        else if ("cp932".equals(mysqlCharset))
            javaCharset = "Cp932";
        else if ("dec8".equals(mysqlCharset))
            javaCharset = "";
        else if ("eucjpms".equals(mysqlCharset))
            javaCharset = "";
        else if ("euckr".equals(mysqlCharset))
            javaCharset = "EUC_KR";
        else if ("gb2312".equals(mysqlCharset))
            javaCharset = "EUC_CN";
        else if ("gbk".equals(mysqlCharset))
            javaCharset = "GBK";
        else if ("geostd8".equals(mysqlCharset))
            javaCharset = "";
        else if ("greek".equals(mysqlCharset))
            javaCharset = "ISO8859_7";
        else if ("hebrew".equals(mysqlCharset))
            javaCharset = "ISO8859_8";
        else if ("hp8".equals(mysqlCharset))
            javaCharset = "";
        else if ("keybcs2".equals(mysqlCharset))
            javaCharset = "";
        else if ("koi8r".equals(mysqlCharset))
            javaCharset = "";
        else if ("koi8u".equals(mysqlCharset))
            javaCharset = "";
        else if ("latin1".equals(mysqlCharset))
            javaCharset = "ISO8859_1";
        else if ("latin2".equals(mysqlCharset))
            javaCharset = "ISO8859_2";
        else if ("latin5".equals(mysqlCharset))
            javaCharset = "ISO8859_5";
        else if ("latin7".equals(mysqlCharset))
            javaCharset = "ISO8859_7";
        else if ("macce".equals(mysqlCharset))
            javaCharset = "MacCentralEurope";
        else if ("macroman".equals(mysqlCharset))
            javaCharset = "MacRoman";
        else if ("sjis".equals(mysqlCharset))
            javaCharset = "SJIS";
        else if ("swe7".equals(mysqlCharset))
            javaCharset = "";
        else if ("tis620".equals(mysqlCharset))
            javaCharset = "TIS620";
        else if ("ucs2".equals(mysqlCharset))
            javaCharset = "UnicodeBig";
        else if ("ujis".equals(mysqlCharset))
            javaCharset = "EUC_JP";
        else if ("utf8".equals(mysqlCharset))
            javaCharset = "UTF-8";
        else if ("utf8mb4".equals(mysqlCharset))
            javaCharset = "UTF-8";
        else if ("utf16".equals(mysqlCharset))
            javaCharset = "UTF-16";
        else if ("utf32".equals(mysqlCharset))
            javaCharset = "UTF-32";
        else {
            logger.warn("Unknown charset: index=" + index + " mysqlCharset=" + mysqlCharset);
            javaCharset = null;
        }

        charsets[index] = new CharsetInfo(index, mysqlCharset, mysqlCollation, javaCharset);
    }

    public static double arr2double(byte[] arr, int start) {
        int i = 0;
        int len = 8;
        int cnt = 0;
        byte[] tmp = new byte[len];
        for (i = start; i < (start + len); i++) {
            tmp[cnt] = arr[i];
            cnt++;
        }
        long accum = 0;
        i = 0;
        for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
            accum |= ((long) (tmp[i] & 0xff)) << shiftBy;
            i++;
        }
        return Double.longBitsToDouble(accum);
    }

    public static float float4ToFloat(byte[] buf, int off) throws IOException {
        float value;
        long bits = LittleEndianConversion.convertNBytesToLong_2(buf, off, 4);

        value = Float.intBitsToFloat((int) bits);

        return value;
    }

    public static double double8ToDouble(byte[] buf, int off) throws IOException {
        double value;

        long bits = LittleEndianConversion.convertNBytesToLong_2(buf, off, 8);

        value = arr2double(buf, off);
        value = Double.longBitsToDouble(bits);

        return value;
    }

    private static int unsignedByteToInt(byte b) {
        return 0xFF & b;
    }

    /*
     * Reads unsigned integer without swapping bytes (e.g. 0x0007 == 7) from no
     * more than 4 bytes
     */
    public static long ulNoSwapToInt(byte[] buf, int off, int len) {
        long ret = 0;
        for (int i = off; i < (off + len); i++) {
            int val = unsignedByteToInt(buf[i]);
            ret = (ret << 8) + val;
        }
        return ret;
    }

    public static String convertDecimalToString(byte[] buffer, int offset, int length) {
        //
        // Decimal representation in binlog seems to be as follows:
        // 1 byte - 'precision'
        // 1 byte - 'scale'
        // remaining n bytes - integer such that value = n / (10^scale)
        // Integer is represented as follows:
        // 1st bit - sign such that set == +, unset == -
        // every 4 bytes represent 9 digits in big-endian order, so that if
        // you print the values of these quads as big-endian integers one after
        // another, you get the whole number string representation in decimal.
        // What remains is to put a sign and a decimal dot.
        // 13 0a 80 00 00 05 1b 38 b0 60 00 means:
        // 0x13 - precision = 19
        // 0x0a - scale = 10
        // 0x80 - positive
        // 0x00000005 0x1b38b060 0x00
        // 5 456700000 0
        // 54567000000 / 10^{10} = 5.4567
        //
        // int_size below shows how long is integer part
        //

        int precision = buffer[offset];
        int scale = buffer[offset + 1];
        offset = offset + 2; // offset of the number part

        int intg = precision - scale;
        int intg0 = intg / DIG_PER_INT32;
        int frac0 = scale / DIG_PER_INT32;
        int intg0x = intg - intg0 * DIG_PER_INT32;
        int frac0x = scale - frac0 * DIG_PER_INT32;

        int sign = (buffer[offset] & 0x80) != 0x80 ? 1 : -1;

        // how many bytes are used to represent given amount of digits?
        int integerSize = intg0 * SIZE_OF_INT32 + dig2bytes[intg0x];
        int decimalSize = frac0 * SIZE_OF_INT32 + dig2bytes[frac0x];
        int bin_size = integerSize + decimalSize; // total bytes
        byte[] d_copy = new byte[bin_size];

        if (bin_size > (length - 2)) {
            throw new ArrayIndexOutOfBoundsException("Calculated bin_size: " + bin_size + ", available bytes: " + (length - 2));
        }

        for (int i = 0; i < bin_size; i++) {
            d_copy[i] = buffer[offset + i];
        }
        // clear sign bit
        d_copy[0] = (byte) (d_copy[0] & 0x7F);

        // first digits of integer part are special:
        // no need for leading zeroes and sign must be present
        offset = (integerSize < SIZE_OF_INT32) ? integerSize : SIZE_OF_INT32;
        int int32 = BigEndianConversion.convertNBytesToInt(d_copy, 0, offset);
        String num = Integer.toString(int32);
        if (sign < 0) {
            num = "-" + num;
        }

        // remaining integer part
        num = num + BigEndianConversion.convertNBytesToString(d_copy, offset, integerSize - offset);
        // decimal point
        num = num + ".";
        // fraction part
        num = num + BigEndianConversion.convertNBytesToString(d_copy, integerSize, decimalSize);

        return num;
    }

    public static long[] decodePackedInteger(byte[] buffer, int position) throws IOException {
        /**
         * A Packed Integer has the capacity of storing up to 8-byte integers,
         * while small integers still can use 1, 3, or 4 bytes. The value of the
         * first byte determines how to read the number, according to the
         * following table.
         * <ul>
         * <li>0-250 The first byte is the number (in the range 0-250). No
         * additional bytes are used.</li>
         * <li>252 Two more bytes are used. The number is in the range
         * 251-0xffff.</li>
         * <li>253 Three more bytes are used. The number is in the range
         * 0xffff-0xffffff.</li>
         * <li>254 Eight more bytes are used. The number is in the range
         * 0xffffff-0xffffffffffffffff.</li>
         * </ul>
         * That representation allows a first byte value of 251 to represent the
         * SQL NULL value.
         */
        long len;
        long ret[] = new long[2];

        if (unsignedByteToInt(buffer[position]) < 251) {
            len = buffer[position];
            position++;
            ret[0] = len;
            ret[1] = position;
            return ret;
        }
        switch (unsignedByteToInt(buffer[position])) {
        case 251:
            position++;
            len = NULL_LENGTH;
            break;
        case 252:
            len = LittleEndianConversion.convert2BytesToInt_2(buffer, position + 1);
            position += 3;
            break;
        case 253:
            len = LittleEndianConversion.convert3BytesToInt(buffer, position + 1);
            position += 4;
            break;
        default:
            // TODO this looks wrong !!! try to find a test that shows this
            // is not correct
            len = unsignedByteToInt(buffer[position]);
            len = LittleEndianConversion.convert4BytesToLong(buffer, position);
            len = LittleEndianConversion.convert4BytesToLong(buffer, position + 1);
            position += 9;
            break;
        }

        ret[0] = len;
        ret[1] = position;
        return ret;
    }

    public static void setBitField(BitSet bitset, byte[] buffer, int pos, int length) {
        int i;
        int byteIdx = pos;
        int b = 0;
        int rightBit = 0x01;
        int bitMask = 0;
        for (i = 0; i < length; i++) {
            if (i % 8 == 0) {
                b = buffer[byteIdx] & 0xFF;
                byteIdx++;
                bitMask = rightBit;
            } else {
                bitMask = bitMask << 1;
            }
            if ((bitMask & b) == bitMask) {
                bitset.set(i);
            } else {
                bitset.clear(i);
            }
        }
    }

    public static String getMySQLCharset(int id) {
        if (id >= 0 && id < charsets.length) {
            CharsetInfo charsetInfo = charsets[id];
            if (charsetInfo == null)
                logger.warn("Unknown character set id requested: " + id);
            else
                return charsetInfo.mysqlCharset;
        } else {
            logger.warn("Unknown character set id requested: " + id);
        }
        return "";
    }

    public static String getMySQLCollation(int id) {
        if (id >= 0 && id < charsets.length) {
            CharsetInfo charsetInfo = charsets[id];
            if (charsetInfo == null)
                logger.warn("Unknown character set id requested: " + id);
            else
                return charsetInfo.mysqlCollation;
        } else {
            logger.warn("Unknown character set id requested: " + id);
        }
        return "";
    }

    public static String getJavaCharset(int id) {
        if (id >= 0 && id < charsets.length) {
            CharsetInfo charsetInfo = charsets[id];
            if (charsetInfo == null)
                logger.warn("Unknown character set id requested: " + id);
            else
                return charsetInfo.javaCharset;
        } else {
            logger.warn("Unknown character set id requested: " + id);
        }
        return "";
    }

}
