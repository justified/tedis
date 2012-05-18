/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import java.io.EOFException;
import java.io.IOException;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.conf.ReplicatorRuntime;
import com.taobao.common.tedis.replicator.extractor.mysql.conversion.LittleEndianConversion;

public abstract class LogEvent {
    static Logger logger = Logger.getLogger(LogEvent.class);

    protected long execTime;
    protected int type;
    protected Timestamp when;
    protected int serverId;

    protected int logPos;
    protected int flags;

    protected boolean threadSpecificEvent = false;

    public LogEvent() {
        type = MysqlBinlog.START_EVENT_V3;
    }

    public LogEvent(byte[] buffer, FormatDescriptionLogEvent descriptionEvent, int eventType) throws ReplicatorException {
        type = eventType;

        try {
            when = new Timestamp(1000 * LittleEndianConversion.convert4BytesToLong(buffer, 0));
            serverId = (int) LittleEndianConversion.convert4BytesToLong(buffer, MysqlBinlog.SERVER_ID_OFFSET);
            if (descriptionEvent.binlogVersion == 1) {
                logPos = 0;
                flags = 0;
                return;
            }

            /* 4.0 or newer */
            logPos = (int) LittleEndianConversion.convert4BytesToLong(buffer, MysqlBinlog.LOG_POS_OFFSET);
            /*
             * If the log is 4.0 (so here it can only be a 4.0 relay log read by
             * the SQL thread or a 4.0 master binlog read by the I/O thread),
             * log_pos is the beginning of the event: we transform it into the
             * end of the event, which is more useful. But how do you know that
             * the log is 4.0: you know it if description_event is version 3and
             * you are not reading a Format_desc (remember that mysqlbinlog
             * starts by assuming that 5.0 logs are in 4.0 format, until it
             * finds a Format_desc).
             */

            if ((descriptionEvent.binlogVersion == 3) && (buffer[MysqlBinlog.EVENT_TYPE_OFFSET] < MysqlBinlog.FORMAT_DESCRIPTION_EVENT) && (logPos > 0)) {
                /*
                 * If log_pos=0, don't change it. log_pos==0 is a marker to mean
                 * "don't change rli->group_master_log_pos" (see
                 * inc_group_relay_log_pos()). As it is unreal log_pos, adding
                 * the event len's is nonsense. For example, a fake Rotate event
                 * should not have its log_pos (which is 0) changed or it will
                 * modify Exec_master_log_pos in SHOW SLAVE STATUS, displaying a
                 * nonsense value of (a non-zero offset which does not exist in
                 * the master's binlog, so which will cause problems if the user
                 * uses this value in CHANGE MASTER).
                 */
                logPos += LittleEndianConversion.convert4BytesToLong(buffer, MysqlBinlog.EVENT_LEN_OFFSET);
            }
            if (logger.isDebugEnabled())
                logger.debug("log_pos: " + logPos);

            flags = LittleEndianConversion.convert2BytesToInt(buffer, MysqlBinlog.FLAGS_OFFSET);
            /*
             * TODO LOG_EVENT_THREAD_SPECIFIC_F = 0x4 (New in 4.1.0) Used only
             * by mysqlbinlog (not by the replication code at all) to be able to
             * deal properly with temporary tables. mysqlbinlog displays events
             * from the binary log in printable format, so that you can feed the
             * output into mysql (the command-line interpreter), to achieve
             * incremental backup recovery.
             */
            threadSpecificEvent = ((flags & MysqlBinlog.LOG_EVENT_THREAD_SPECIFIC_F) == MysqlBinlog.LOG_EVENT_THREAD_SPECIFIC_F);
            if (logger.isDebugEnabled())
                logger.debug("Event is thread-specific = " + threadSpecificEvent);

            if ((buffer[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.FORMAT_DESCRIPTION_EVENT) || (buffer[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.ROTATE_EVENT)) {
                /*
                 * These events always have a header which stops here (i.e.
                 * their header is FROZEN).
                 */
                /*
                 * Initialization to zero of all other Log_event members as
                 * they're not specified. Currently there are no such members;
                 * in the future there will be an event UID (but
                 * Format_description and Rotate don't need this UID, as they
                 * are not propagated through --log-slave-updates (remember the
                 * UID is used to not play a query twice when you have two
                 * masters which are slaves of a 3rd master). Then we are done.
                 */
                return;
            }
            /*
             * otherwise, go on with reading the header from buffer (nothing for
             * now)
             */
        } catch (IOException e) {
            throw new MySQLExtractException("log event create failed", e);
        }
    }

    public long getExecTime() {
        return execTime;
    }

    public Timestamp getWhen() {
        return when;
    }

    public static LogEvent readLogEvent(boolean parseStatements, byte[] buffer, int eventLength, FormatDescriptionLogEvent descriptionEvent, boolean useBytesForString) throws ReplicatorException {
        LogEvent event = null;

        switch (buffer[MysqlBinlog.EVENT_TYPE_OFFSET]) {
        case MysqlBinlog.QUERY_EVENT:
            event = new QueryLogEvent(buffer, eventLength, descriptionEvent, parseStatements, useBytesForString);
            break;
        case MysqlBinlog.LOAD_EVENT:
            logger.warn("Skipping unsupported LOAD_EVENT");
            // ev = new Load_log_event(buf, event_len, description_event);
            break;
        case MysqlBinlog.NEW_LOAD_EVENT:
            logger.warn("Skipping unsupported NEW_LOAD_EVENT");
            // ev = new Load_log_event(buf, event_len, description_event);
            break;
        case MysqlBinlog.ROTATE_EVENT:
            event = new RotateLogEvent(buffer, eventLength, descriptionEvent);
            break;
        case MysqlBinlog.SLAVE_EVENT: /* can never happen (unused event) */
            logger.warn("Skipping unsupported SLAVE_EVENT");
            // ev = new Slave_log_event(buf, event_len);
            break;
        case MysqlBinlog.CREATE_FILE_EVENT:
            logger.warn("Skipping unsupported CREATE_FILE_EVENT");
            // ev = new Create_file_log_event(buf, event_len,
            // description_event);
            break;
        case MysqlBinlog.APPEND_BLOCK_EVENT:
            if (logger.isDebugEnabled())
                logger.debug("reading APPEND_BLOCK_EVENT");
            event = new AppendBlockLogEvent(buffer, eventLength, descriptionEvent);
            break;
        case MysqlBinlog.DELETE_FILE_EVENT:
            if (logger.isDebugEnabled())
                logger.debug("reading DELETE_FILE_EVENT");
            event = new DeleteFileLogEvent(buffer, eventLength, descriptionEvent);
            break;
        case MysqlBinlog.EXEC_LOAD_EVENT:
            logger.warn("Skipping unsupported EXEC_LOAD_EVENT");
            break;
        case MysqlBinlog.START_EVENT_V3:
            /* this is sent only by MySQL <=4.x */
            logger.warn("Skipping unsupported START_EVENT_V3");
            break;
        case MysqlBinlog.STOP_EVENT:
            event = new StopLogEvent(buffer, eventLength, descriptionEvent);
            break;
        case MysqlBinlog.INTVAR_EVENT:
            if (logger.isDebugEnabled())
                logger.debug("extracting INTVAR_EVENT");
            event = new IntvarLogEvent(buffer, eventLength, descriptionEvent);
            break;
        case MysqlBinlog.XID_EVENT:
            event = new XidLogEvent(buffer, eventLength, descriptionEvent);
            break;
        case MysqlBinlog.RAND_EVENT:
            event = new RandLogEvent(buffer, eventLength, descriptionEvent);
            break;
        case MysqlBinlog.USER_VAR_EVENT:
            event = new UserVarLogEvent(buffer, eventLength, descriptionEvent);
            break;
        case MysqlBinlog.FORMAT_DESCRIPTION_EVENT:
            event = new FormatDescriptionLogEvent(buffer, eventLength, descriptionEvent);
            break;
        case MysqlBinlog.PRE_GA_WRITE_ROWS_EVENT:
            logger.warn("Skipping unsupported PRE_GA_WRITE_ROWS_EVENT");
            // ev = new Write_rows_log_event_old(buf, event_len,
            // description_event);
            break;
        case MysqlBinlog.PRE_GA_UPDATE_ROWS_EVENT:
            logger.warn("Skipping unsupported PRE_GA_UPDATE_ROWS_EVENT");
            // ev = new Update_rows_log_event_old(buf, event_len,
            // description_event);
            break;
        case MysqlBinlog.PRE_GA_DELETE_ROWS_EVENT:
            logger.warn("Skipping unsupported PRE_GA_DELETE_ROWS_EVENT");
            // ev = new Delete_rows_log_event_old(buf, event_len,
            // description_event);
            break;
        case MysqlBinlog.WRITE_ROWS_EVENT:
            if (logger.isDebugEnabled())
                logger.debug("reading WRITE_ROWS_EVENT");
            event = new WriteRowsLogEvent(buffer, eventLength, descriptionEvent, useBytesForString);
            break;
        case MysqlBinlog.UPDATE_ROWS_EVENT:
            if (logger.isDebugEnabled())
                logger.debug("reading UPDATE_ROWS_EVENT");
            event = new UpdateRowsLogEvent(buffer, eventLength, descriptionEvent, useBytesForString);
            break;
        case MysqlBinlog.DELETE_ROWS_EVENT:
            if (logger.isDebugEnabled())
                logger.debug("reading DELETE_ROWS_EVENT");
            event = new DeleteRowsLogEvent(buffer, eventLength, descriptionEvent, useBytesForString);
            break;
        case MysqlBinlog.TABLE_MAP_EVENT:
            if (logger.isDebugEnabled())
                logger.debug("reading TABLE_MAP_EVENT");
            event = new TableMapLogEvent(buffer, eventLength, descriptionEvent);
            break;
        case MysqlBinlog.BEGIN_LOAD_QUERY_EVENT:
            if (logger.isDebugEnabled())
                logger.debug("reading BEGIN_LOAD_QUERY_EVENT");
            event = new BeginLoadQueryLogEvent(buffer, eventLength, descriptionEvent);
            break;
        case MysqlBinlog.EXECUTE_LOAD_QUERY_EVENT:
            if (logger.isDebugEnabled())
                logger.debug("reading EXECUTE_LOAD_QUERY_EVENT");
            event = new ExecuteLoadQueryLogEvent(buffer, eventLength, descriptionEvent, parseStatements);
            break;
        case MysqlBinlog.INCIDENT_EVENT:
            if (logger.isDebugEnabled())
                logger.warn("Skipping unsupported INCIDENT_EVENT");
            break;
        default:
            logger.warn("Skipping unrecognized binlog event type " + buffer[MysqlBinlog.EVENT_TYPE_OFFSET]);
        }
        return event;
    }

    public static LogEvent readLogEvent(ReplicatorRuntime runtime, BinlogReader position, FormatDescriptionLogEvent descriptionEvent, boolean parseStatements, boolean useBytesForString,
            boolean prefetchSchemaNameLDI) throws ReplicatorException, InterruptedException {
        int eventLength = 0;
        byte[] header = new byte[descriptionEvent.commonHeaderLength];

        try {
            // read the header part
            // timeout is set to 2 minutes.
            readDataFromBinlog(runtime, position, header, 0, header.length, 120);

            // Extract event length
            eventLength = (int) LittleEndianConversion.convert4BytesToLong(header, MysqlBinlog.EVENT_LEN_OFFSET);

            eventLength -= header.length;

            byte[] fullEvent = new byte[header.length + eventLength];

            // read the event data part
            // timeout is set to 2 minutes
            readDataFromBinlog(runtime, position, fullEvent, header.length, eventLength, 120);

            System.arraycopy(header, 0, fullEvent, 0, header.length);

            LogEvent event = readLogEvent(parseStatements, fullEvent, fullEvent.length, descriptionEvent, useBytesForString);

            // If schema name has to be prefetched, check if it is a BEGIN LOAD
            // EVENT
            if (prefetchSchemaNameLDI && event instanceof BeginLoadQueryLogEvent) {
                if (logger.isDebugEnabled())
                    logger.debug("Got Begin Load Query Event - Looking for corresponding Execute Event");

                BeginLoadQueryLogEvent beginLoadEvent = (BeginLoadQueryLogEvent) event;
                // Spawn a new data input stream
                BinlogReader tempPosition = position.clone();
                tempPosition.setEventID(position.getEventID() + 1);
                tempPosition.open();

                if (logger.isDebugEnabled())
                    logger.debug("Reading from " + tempPosition);
                boolean found = false;
                byte[] tmpHeader = new byte[descriptionEvent.commonHeaderLength];

                while (!found) {
                    readDataFromBinlog(runtime, tempPosition, tmpHeader, 0, tmpHeader.length, 60);

                    // Extract event length
                    eventLength = (int) LittleEndianConversion.convert4BytesToLong(tmpHeader, MysqlBinlog.EVENT_LEN_OFFSET) - tmpHeader.length;

                    if (tmpHeader[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.EXECUTE_LOAD_QUERY_EVENT) {
                        fullEvent = new byte[tmpHeader.length + eventLength];
                        readDataFromBinlog(runtime, tempPosition, fullEvent, tmpHeader.length, eventLength, 120);

                        System.arraycopy(tmpHeader, 0, fullEvent, 0, tmpHeader.length);

                        LogEvent tempEvent = readLogEvent(parseStatements, fullEvent, fullEvent.length, descriptionEvent, useBytesForString);

                        if (tempEvent instanceof ExecuteLoadQueryLogEvent) {
                            ExecuteLoadQueryLogEvent execLoadQueryEvent = (ExecuteLoadQueryLogEvent) tempEvent;
                            if (execLoadQueryEvent.getFileID() == beginLoadEvent.getFileID()) {
                                if (logger.isDebugEnabled())
                                    logger.debug("Found corresponding Execute Load Query Event - Schema is " + execLoadQueryEvent.getDefaultDb());
                                beginLoadEvent.setSchemaName(execLoadQueryEvent.getDefaultDb());
                                found = true;
                            }
                        }

                    } else {
                        long skip = 0;
                        while (skip != eventLength) {
                            skip += tempPosition.skip(eventLength - skip);
                        }
                    }
                }
                // Release the file handler
                tempPosition.close();
            }

            return event;
        } catch (EOFException e) {
            throw new MySQLExtractException("EOFException while reading " + eventLength + " bytes from binlog ", e);
        } catch (IOException e) {
            throw new MySQLExtractException("binlog read error", e);
        }
    }

    /**
     * readDataFromBinlog waits for data to be fully written in the binlog file
     * and then reads it.
     *
     * @param runtime
     *            replicator runtime
     * @param dis
     *            Input stream from which data will be read
     * @param data
     *            Array of byte that will contain read data
     * @param offset
     *            Position in the previous array where data should be written
     * @param length
     *            Data length to be read
     * @param timeout
     *            Maximum time to wait for data to be available
     * @throws IOException
     *             if an error occurs while reading from the stream
     * @throws ReplicatorException
     *             if the timeout is reached
     */
    private static void readDataFromBinlog(ReplicatorRuntime runtime, BinlogReader binlog, byte[] data, int offset, int length, int timeout) throws IOException, ReplicatorException {
        boolean alreadyLogged = false;
        int spentTime = 0;
        int timeoutInMs = timeout * 1000;

        while (length > binlog.available()) {
            if (!alreadyLogged) {
                logger.warn("Trying to read more bytes (" + length + ") than available in the file... waiting for data to be available");
                alreadyLogged = true;
            }

            try {
                if (spentTime < timeoutInMs) {
                    Thread.sleep(1);
                    spentTime++;
                } else
                    throw new MySQLExtractException("Timeout while waiting for data : spent more than " + timeout + " seconds while waiting for " + length + " bytes to be available");
            } catch (InterruptedException e) {
            }
        }
        binlog.read(data, offset, length);
    }

    public int getType() {
        return type;
    }

    protected static String hexdump(byte[] buffer, int offset) {
        StringBuffer dump = new StringBuffer();
        if ((buffer.length - offset) > 0) {
            dump.append(String.format("%02x", buffer[offset]));
            for (int i = offset + 1; i < buffer.length; i++) {
                dump.append("_");
                dump.append(String.format("%02x", buffer[i]));
            }
        }
        return dump.toString();
    }

    protected String hexdump(byte[] buffer, int offset, int length) {
        StringBuffer dump = new StringBuffer();

        if (buffer.length >= offset + length) {
            dump.append(String.format("%02x", buffer[offset]));
            for (int i = offset + 1; i < offset + length; i++) {
                dump.append("_");
                dump.append(String.format("%02x", buffer[i]));
            }
        }
        return dump.toString();
    }

    public static String hexdump(byte[] buffer) {
        // TODO Auto-generated method stub
        return hexdump(buffer, 0);
    }

}
