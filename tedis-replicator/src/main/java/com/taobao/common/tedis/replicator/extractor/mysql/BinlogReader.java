/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.extractor.mysql.conversion.LittleEndianConversion;
import com.taobao.common.tedis.replicator.io.BufferedFileDataInput;

public class BinlogReader implements FilenameFilter, Cloneable {
    static Logger logger = Logger.getLogger(MySQLExtractor.class);

    // Stream from which we are reading.
    private BufferedFileDataInput bfdi;

    // Binlog file name and directory.
    private String fileName;
    private String directory;

    // Binlog file base name.
    private String baseName;

    // Start position. We seek to this after open if greater than 0.
    private long startPosition;

    // Binlog version. This must be set externally by clients after reading
    // the header.
    private int version = MysqlBinlog.VERSION_NONE;

    // Id of last event read.
    private int eventID;

    // Buffer size for reads.
    private int bufferSize = 64000;

    public BinlogReader(String directory, String baseName, int bufferSize) {
        this(0, null, directory, baseName, bufferSize);
    }

    public BinlogReader(long start, String fileName, String directory, String baseName, int bufferSize) {
        this.bfdi = null;
        this.startPosition = start;
        this.eventID = 0;
        this.fileName = fileName;
        this.directory = directory;
        this.baseName = baseName;
        this.bufferSize = bufferSize;
    }

    public BinlogReader clone() {
        long offset = bfdi == null ? 0 : bfdi.getOffset();
        BinlogReader cloned = new BinlogReader(offset, fileName, directory, baseName, bufferSize);

        // Set last ID read.
        cloned.setEventID(eventID);

        return cloned;
    }

    void open() throws ReplicatorException, InterruptedException {
        try {
            // Check for safety conditions.
            if (getFileName() == null) {
                throw new MySQLExtractException("No binlog file specified");
            }
            if (bfdi != null) {
                throw new MySQLExtractException("Attempt to open binlog twice: " + this.fileName);
            }

            // Hack to avoid crashing during log rotate. MySQL seems to write
            // log rotate event in the old file before creating new file. We
            // wait for a few seconds, polling file every 10 msecs.
            File file = new File(getDirectory() + File.separator + getFileName());
            int tryCnt = 0;
            while (file.exists() == false && tryCnt++ < 500) {
                Thread.sleep(10);
            }

            if (logger.isDebugEnabled())
                logger.debug("Opening file " + file.getName() + " with buffer = " + bufferSize);

            bfdi = new BufferedFileDataInput(file, bufferSize);

            // Validate the file magic number.
            byte magic[] = new byte[MysqlBinlog.BIN_LOG_HEADER_SIZE];
            try {
                if (available() < magic.length) {
                    throw new MySQLExtractException("Failed reading header;  Probably an empty file: " + getBaseName());
                }
                read(magic);
                if (!Arrays.equals(magic, MysqlBinlog.BINLOG_MAGIC)) {
                    throw new MySQLExtractException("File is not a binary log file - found : " + LogEvent.hexdump(magic) + " / expected : " + LogEvent.hexdump(MysqlBinlog.BINLOG_MAGIC));
                }
            } catch (IOException e) {
                throw new MySQLExtractException("Failed reading binlog file header: " + getBaseName(), e);
            }

            // Figure out the binlog format and mark accordingly. Here are the
            // rules for distinguishing formats.
            //
            // Binlog V1 - MySQL 3.23. Starts with 69-byte START_EVENT_V3.
            // Binlog V3 - MySQL 4.0/4.1. Starts with 75-byte START_EVENT_V3.
            // Binlog V4 - MySQL 5.0+. Starts with FORMAT_DESCRIPTION_EVENT.
            //
            // Detailed rules for determining type can be found at the following
            // URL:
            // http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log#Determining_the_Binary_Log_Version

            // Mark a reset point to return to after readahead to deduce the
            // binlog version.
            mark(2048);

            // Read first for fields, which are common to all events.
            byte[] header = new byte[MysqlBinlog.PROBE_HEADER_LEN];
            bfdi.readFully(header);
            int typeCode = header[4];
            int eventLength = (int) LittleEndianConversion.convert4BytesToLong(header, MysqlBinlog.EVENT_LEN_OFFSET);

            // Apply rules for distinguishing binlog type, generating
            // exceptions if we seem to be confused.
            if (typeCode == MysqlBinlog.START_EVENT_V3) {
                // Check event length to distinguish between V1 and V3.
                if (eventLength == 69) {
                    version = MysqlBinlog.BINLOG_V1;
                    if (logger.isDebugEnabled())
                        logger.debug("Binlog format is V1");
                } else if (eventLength == 75) {
                    version = MysqlBinlog.BINLOG_V3;
                    if (logger.isDebugEnabled())
                        logger.debug("Binlog format is V3");
                } else {
                    throw new MySQLExtractException("Unexpected start event length: file=" + this.fileName + " length=" + eventLength);
                }
            } else if (typeCode == MysqlBinlog.FORMAT_DESCRIPTION_EVENT) {
                version = MysqlBinlog.BINLOG_V4;
                if (logger.isDebugEnabled())
                    logger.debug("Binlog format is V4");
            } else if (typeCode == MysqlBinlog.ROTATE_EVENT) {
                version = MysqlBinlog.BINLOG_V3;
                if (logger.isDebugEnabled())
                    logger.debug("Binlog format is V3 (special case w/ rotate event)");
            } else {
                throw new MySQLExtractException("Unexpected start event type code: file=" + this.fileName + " type code=" + typeCode);
            }

            // If we have predefined position to start from, let's skip until
            // the position. This situation can happen if the extractor is
            // cloned and we need to seek to the correct read position.
            // Otherwise just reset.
            if (startPosition >= bfdi.getOffset()) {
                bfdi.seek(startPosition);
            } else {
                bfdi.reset();
            }
        } catch (FileNotFoundException e) {
            throw new MySQLExtractException("Unable to open binlog file: ", e);
        } catch (IOException e) {
            throw new MySQLExtractException("Unable to scan binlog file", e);
        }
    }

    public boolean isOpen() {
        return (bfdi != null);
    }

    public void close() throws ReplicatorException {
        if (bfdi != null) {
            bfdi.close();
            bfdi = null;
        }
        setStartPosition(0);
        setEventID(0);
        setFileName(null);
    }

    public int available() throws IOException {
        return bfdi.available();
    }

    public int waitAvailable(int requested, int waitMillis) throws IOException, InterruptedException {
        return bfdi.waitAvailable(requested, waitMillis);
    }

    public long skip(long bytes) throws IOException {
        return bfdi.skip(bytes);
    }

    public void mark(int readLimit) {
        bfdi.mark(readLimit);
    }

    public void reset() throws IOException, InterruptedException {
        bfdi.reset();
    }

    public void read(byte[] buf) throws IOException {
        bfdi.readFully(buf);
    }

    public void read(byte[] buf, int offset, int len) throws IOException {
        bfdi.readFully(buf, offset, len);
    }

    public long readLong() throws IOException {
        return bfdi.readLong();
    }

    public int readInt() throws IOException {
        return bfdi.readInt();
    }

    public byte readByte() throws IOException {
        return bfdi.readByte();
    }

    public void setStartPosition(long newPosition) {
        startPosition = newPosition;
    }

    public long getPosition() {
        if (bfdi != null)
            return bfdi.getOffset();
        else
            return startPosition;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return (fileName);
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public String getDirectory() {
        return (directory);
    }

    public boolean accept(File dir, String name) {
        if (name.startsWith(baseName))
            return true;
        return false;
    }

    public String getBaseName() {
        return baseName;
    }

    public void setBaseName(String baseName) {
        this.baseName = baseName;
    }

    public int getEventID() {
        return eventID;
    }

    public void setEventID(int eventID) {
        this.eventID = eventID;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return fileName + " (" + getPosition() + ")";
    }
}
