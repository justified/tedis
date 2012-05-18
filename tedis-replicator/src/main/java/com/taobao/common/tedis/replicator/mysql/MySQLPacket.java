/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.mysql;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.log4j.Logger;

/**
 * A MySQL packet with helper functions to ease the reading and writting of
 * bytes, integers, long integers, strings...
 *
 * @author <a href="mailto:csaba.simon@continuent.com">Csaba Simon</a>
 * @author <a href="gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version $Id: $
 */
public class MySQLPacket {
    /** Length of the packet header */
    public static final int HEADER_LENGTH = 4;
    /** Maximum packet length */
    public static final int MAX_LENGTH = 0x00ffffff;

    private static final long NULL_LENGTH = -1;

    private static final Logger logger = Logger.getLogger(MySQLPacket.class);
    /** Header + data buffer */
    private byte[] byteBuffer;

    /** Cursor position */
    private int position;
    /** Data size */
    private int dataLength = 0;
    /** The input stream used to read this packet */
    InputStream inputStream = null;

    /**
     * Creates a new <code>MySQLPacket</code> object
     *
     * @param buffer
     *            the buffer
     * @param packetNumber
     *            the packet number
     */
    public MySQLPacket(int dataLength, byte[] buffer, byte packetNumber) {
        this.byteBuffer = buffer;
        this.byteBuffer[3] = packetNumber;
        this.dataLength = dataLength;
        this.position = HEADER_LENGTH;
    }

    /**
     * Creates a new <code>MySQLPacket</code> object
     *
     * @param size
     *            the size of the buffer
     * @param packetNumber
     *            the packet number
     */
    public MySQLPacket(int size, byte packetNumber) {
        if (size < HEADER_LENGTH) {
            this.byteBuffer = new byte[HEADER_LENGTH + size];
        } else {
            this.byteBuffer = new byte[size];
        }
        this.byteBuffer[3] = packetNumber;
        this.position = HEADER_LENGTH;
    }

    /**
     * Reads a MySQL packet from the input stream.
     *
     * @param in
     *            the data input stream from where we read the MySQL packet
     * @return a MySQLPacket object or null if the MySQL packet cannot be read
     */
    public static MySQLPacket readPacket(InputStream in) {
        if (in == null) {
            return null;
        }
        try {
            int mask = 0xff;
            int packetLen1 = in.read();
            int packetLen2 = in.read();
            int packetLen3 = in.read();
            int packetLen = (packetLen1 & mask) | (packetLen2 & mask) << 8 | (packetLen3 & mask) << 16;
            int packetNumber = in.read();
            // This is ok, no more packet on the line
            if (packetLen1 == -1) {
                logger.debug("Reached end of input stream while reading packet");
                return null;
            }
            // This is bad, client went away
            if (packetLen2 == -1 || packetLen3 == -1 || packetNumber == -1) {
                throw new EOFException("Reached end of input stream.");
            }

            // read the body of the packet
            byte[] packetData = new byte[packetLen + HEADER_LENGTH];
            // copy header
            packetData[0] = (byte) packetLen1;
            packetData[1] = (byte) packetLen2;
            packetData[2] = (byte) packetLen3;
            packetData[3] = (byte) packetNumber;

            // read() returns the number of actual bytes read, which might be
            // less that the desired length this loop ensures that the whole
            // packet is read
            int n = 0;
            while (n < packetLen) {
                int count = in.read(packetData, HEADER_LENGTH + n, packetLen - n);

                if (count < 0) {
                    throw new EOFException("Reached end of input stream.");
                }

                n += count;
            }
            MySQLPacket p = new MySQLPacket(packetLen, packetData, (byte) packetNumber);
            p.setInputStream(in);
            return p;
        } catch (SocketTimeoutException e) {
            logger.warn("Socket timeout expired, closing connection");
        } catch (SocketException e) {
            logger.warn("Seems socket closed because of the Server stop event. Just ignore this.");
        } catch (IOException e) {
            logger.error("I/O error while reading from client socket", e);
        }

        return null;
    }

    /**
     * Returns the raw byte buffer.
     */
    public byte[] getByteBuffer() {
        return byteBuffer;
    }

    /**
     * Returns the packet number.
     *
     * @return the packet number
     */
    public byte getPacketNumber() {
        return this.byteBuffer[3];
    }

    /**
     * Gets the current length of the byteBuffer
     *
     * @return the byte buffer length
     */
    public int getDataLength() {
        return this.dataLength;
    }

    /**
     * Retrieves the current byte buffer cursor position
     *
     * @return the position field
     */
    public int getPacketPosition() {
        return this.position;
    }

    /**
     * Changes the byte buffer cursor position
     *
     * @param pos
     *            the new position
     */
    public void setPacketPosition(int pos) {
        this.position = pos;
    }

    /**
     * When streaming a packet, some data has possibly been already read in it.
     * In order to send it, we need to scroll the cursor to the last data byte
     * position
     */
    public void preparePacketForStreaming() {
        setPacketPosition(getDataLength() + HEADER_LENGTH);
    }

    /**
     * Whether or not this packet is a MySQL "OK" packet
     *
     * @return true if this packet is a OK packet
     */
    public boolean isOK() {
        return this.byteBuffer[4] == (byte) 0x00;
    }

    /**
     * Whether or not this packet is a MySQL "ERROR" packet
     *
     * @return true if this packet is a ERROR packet
     */
    public boolean isError() {
        return this.byteBuffer[4] == (byte) 0xFF;
    }

    /**
     * Whether or not this packet is a MySQL "EOF" packet
     *
     * @return true if this packet is a EOF packet
     */
    public boolean isEOF() {
        return this.byteBuffer[4] == (byte) 0xFE;
    }

    /**
     * Tests special packet "server status" flag to tell whether or not more
     * results exist on the server
     *
     * @return true if SERVER_MORE_RESULTS_EXISTS is set, false otherwise
     */
    public boolean hasMoreResults() {
        return (this.byteBuffer[position - 2] & MySQLConstants.SERVER_MORE_RESULTS_EXISTS) != 0;
    }

    /**
     * Tests special packet "server status" flag to tell whether or not a cursor
     * exists for this result
     *
     * @return true if SERVER_STATUS_CURSOR_EXISTS is set, false otherwise
     */
    public boolean hasCursor() {
        return (this.byteBuffer[position - 2] & MySQLConstants.SERVER_STATUS_CURSOR_EXISTS) != 0;
    }

    /**
     * Tests special packet "server status" flag to tell whether or not the end
     * of result set has been reached
     *
     * @return true if SERVER_STATUS_LAST_ROW_SENT is set, false otherwise
     */
    public boolean hasLastRowSent() {
        return (this.byteBuffer[position - 2] & MySQLConstants.SERVER_STATUS_LAST_ROW_SENT) != 0;
    }

    /**
     * Returns the number of bytes remaining in the buffer
     *
     * @return remaining bytes to read
     */
    public int getRemainingBytes() {
        return this.byteBuffer.length - this.position;
    }

    /**
     * Returns one byte from the buffer.
     *
     * @return one byte from the buffer
     */
    public byte getByte() {
        return this.byteBuffer[this.position++];
    }

    /**
     * Reads one byte from the buffer considering it as an unsigned value and
     * returns a corresponding short
     *
     * @return one byte from the buffer
     */
    public short getUnsignedByte() {
        int firstByte = this.byteBuffer[this.position++] & 0xff;
        short ret = (short) firstByte;
        return ret;
    }

    /**
     * Returns len bytes from the buffer.
     *
     * @param len
     *            the number of bytes to return
     * @return len bytes from the buffer
     */
    public byte[] getBytes(int len) {
        byte[] b = new byte[len];

        System.arraycopy(this.byteBuffer, this.position, b, 0, len);
        this.position += len;

        return b;
    }

    /**
     * Aka. getUnsignedInt16(), returns two consecutive bytes from the buffer
     * considering them as an unsigned value
     *
     * @return an integer corresponding to 2 buffer bytes treated as an unsigned
     *         value
     */
    public int getUnsignedShort() {
        byte[] b = this.byteBuffer;

        return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8);
    }

    /**
     * Aka. getInt16(), returns two consecutive bytes from the buffer
     * considering them as signed value<br>
     *
     * @return an integer corresponding to the 2 bytes treated as a signed value
     */
    public short getShort() {
        byte[] b = this.byteBuffer;
        byte firstbyte = b[this.position++];
        byte secondbyte = b[this.position++];
        return (short) ((secondbyte << 8) | firstbyte & 0xff);
    }

    /**
     * Returns a len encoded byte array from the buffer.
     *
     * @return a len encoded byte array from the buffer
     */
    public byte[] getLenEncodedBytes() {
        long len = this.readFieldLength();

        if (len == NULL_LENGTH) {
            return null;
        }

        if (len == 0) {
            return new byte[0];
        }

        return getBytes((int) len);
    }

    /**
     * Returns four consecutive bytes as an integer (MySQL long) from the
     * buffer.
     *
     * @return four consecutive bytes as an integer (MySQL long) from the buffer
     */
    public int getInt32() {
        byte[] b = this.byteBuffer;

        return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8) | ((b[this.position++] & 0xff) << 16) | ((b[this.position++] & 0xff) << 24);
    }

    /**
     * Returns four consecutive bytes as an integer (MySQL long) from the
     * buffer.
     *
     * @return an integer (MySQL long) based from the buffer
     */
    public long getUnsignedInt32() {
        byte[] b = this.byteBuffer;
        long firstbyte = b[this.position++] & 0xFF;
        long secondbyte = b[this.position++] & 0xFF;
        long thirdbyte = b[this.position++] & 0xFF;
        long forthbyte = b[this.position++] & 0xFF;
        return (long) (forthbyte << 24 | thirdbyte << 16 | secondbyte << 8 | firstbyte) & 0xFFFFFFFFL;
    }

    /**
     * Returns three consecutive bytes as an integer (MySQL longint) from the
     * buffer considering it as an unsigned.
     *
     * @return an integer (MySQL longint) from the buffer treated as an unsigned
     */
    public int getUnsignedInt24() {
        byte[] b = this.byteBuffer;

        return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8) | ((b[this.position++] & 0xff) << 16);
    }

    /**
     * Returns three consecutive bytes as an integer (MySQL longint) from the
     * buffer.
     *
     * @return three consecutive bytes as an integer (MySQL longint) from the
     *         buffer.
     */
    public int getInt24() {
        byte[] b = this.byteBuffer;

        return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8) | ((b[this.position++]) << 16);
    }

    /**
     * Returns eight consecutive bytes as a long (MySQL longlong) from the
     * buffer.
     *
     * @return eight consecutive bytes as a long (MySQL longlong) from the
     *         buffer
     */
    public long getLong() {
        byte[] b = this.byteBuffer;

        return (b[this.position++] & 0xff) | ((long) (b[this.position++] & 0xff) << 8) | ((long) (b[this.position++] & 0xff) << 16) | ((long) (b[this.position++] & 0xff) << 24)
                | ((long) (b[this.position++] & 0xff) << 32) | ((long) (b[this.position++] & 0xff) << 40) | ((long) (b[this.position++] & 0xff) << 48) | ((long) (b[this.position++] & 0xff) << 56);
    }

    /**
     * Returns eight consecutive bytes from the buffer (MySQL longlong) treated
     * as unsigned value into a big integer .
     *
     * @return the corresponding bigInteger
     */
    public BigInteger getUnsignedLong() {
        // create a byte array that BigInteger can read
        byte[] byteArray = new byte[8];
        int sign = 0; // will become 1 if the number is non-zero

        // invert endianess and check if number is zero
        for (int byteIndex = 0; byteIndex < 8; byteIndex++) {
            // put most significant by first
            byteArray[byteIndex] = (byte) (this.byteBuffer[this.position + 7 - byteIndex] & 0xff);
            // make positive if one of the bits is not zero (done only once)
            if (sign == 0 && byteArray[byteIndex] != 0)
                sign = 1;
        }
        this.position += 8;
        BigInteger ret = new BigInteger(sign, byteArray);
        return ret;
    }

    /**
     * Returns four bytes from the buffer as a float
     */
    public float getFloat() {
        return Float.intBitsToFloat(getInt32());
    }

    /**
     * Returns eight bytes from the buffer as a float
     */
    public double getDouble() {
        return Double.longBitsToDouble(getLong());
    }

    /**
     * Returns a string from the buffer.
     *
     * @return a string from the buffer
     */
    public String getString() {
        int i = this.position;
        int len = 0;
        int maxLen = this.byteBuffer.length;

        while ((i < maxLen) && (this.byteBuffer[i] != 0)) {
            len++;
            i++;
        }

        return getString(len);
    }

    /**
     * Returns a len length string from the buffer.
     *
     * @param len
     *            the length of the string
     * @return a len length string from the buffer
     */
    public String getString(int len) {
        int maxLen = this.byteBuffer.length - this.position;

        String s = new String(this.byteBuffer, this.position, len < maxLen ? len : maxLen);
        this.position += (len + 1);

        return s;
    }

    /**
     * Returns a string from the buffer, where the encoded size is right before
     * the actual string
     *
     * @param stopAtNullChar
     *            whether or not to consider the string ends with the null
     *            character. When false, the string length will always be the
     *            decoded length
     * @return string with decoded length
     */
    public String getLenEncodedString(boolean stopAtNullChar) {
        int leftBytes = getRemainingBytes();
        if (leftBytes < 1)
            return "";

        long announcedLength = readFieldLength();
        if (announcedLength > Integer.MAX_VALUE)
            logger.warn("String length bug here!");
        // make sure client did not give a buggy length
        if (announcedLength > leftBytes)
            announcedLength = leftBytes;
        int actualLength = (int) announcedLength;
        if (stopAtNullChar) {
            int i = this.position;
            int untilNullCharLen = 0;
            while ((untilNullCharLen < announcedLength) && (this.byteBuffer[i] != 0)) {
                untilNullCharLen++;
                i++;
            }
            actualLength = untilNullCharLen;
        }
        String s = new String(this.byteBuffer, this.position, actualLength);
        // even if we constructed a smaller string, we have to discard what's
        // after the null char
        this.position += announcedLength;
        return s;
    }

    /**
     * Reads a time from stream given its length and returns the corresponding
     * {@link Time}
     *
     * @param length
     *            size of the time data
     * @return jdbc Time decoded from stream
     */
    public Time getTime(int length) {
        long millis = 0;
        boolean neg = false;
        if (length >= 8) {
            neg = (getByte() != 0);
            if (logger.isTraceEnabled())
                logger.trace("neg=" + neg);
            int day = getInt32();
            if (logger.isTraceEnabled())
                logger.trace("day=" + day);
            millis = getHourMinSec();
            if (length > 8) {
                int mil = getInt32();
                if (logger.isTraceEnabled())
                    logger.trace("millis =" + mil % 1000);
                // MySQL ignores millis > 1000 (don't add the corresponding
                // seconds) => %1000
                millis += mil % 1000;
            }
            // TODO: as jdbc time cannot handle hours over 24, we ignore it =>
            // document this fact
            // if (hour > 838) {
            // hour = 838;
            // min = 59;
            // sec = 59;
            // }
            // millis += day * 24 * 60 * 60 * 1000;
        }
        return new Time(neg ? -millis : millis);
    }

    /**
     * Reads hours, minutes and seconds from stream and returns the
     * corresponding number of milliseconds
     *
     * @return milliseconds decoded from stream
     */
    public long getHourMinSec() {
        int hour = getByte();
        if (logger.isTraceEnabled())
            logger.trace("hour=" + hour);
        int min = getByte();
        if (logger.isTraceEnabled())
            logger.trace("min=" + min);
        int sec = getByte();
        if (logger.isTraceEnabled())
            logger.trace("sec=" + sec);
        return sec * 1000 + min * 60 * 1000 + hour * 60 * 60 * 1000;
    }

    /**
     * Reads a date from stream given its length and returns the corresponding
     * {@link Date}
     *
     * @param length
     *            size of the date data
     * @return jdbc Date decoded from stream
     */
    public Date getDate(int length) {
        int year = 0;
        int month = 0;
        int day = 0;
        if (length >= 4) {
            year = getUnsignedShort();
            if (logger.isTraceEnabled())
                logger.trace("year=" + year);
            month = getByte();
            if (logger.isTraceEnabled())
                logger.trace("month=" + month);
            day = getByte();
            if (logger.isTraceEnabled())
                logger.trace("day=" + day);
        }
        // This is the easier way, probably not the fastest
        String date = "" + year + "-" + month + "-" + day;
        return Date.valueOf(date);
    }

    /**
     * Put a byte in the buffer.
     *
     * @param b
     *            the byte to put in the buffer
     */
    public void putByte(byte b) {
        ensureCapacity(1);

        this.byteBuffer[this.position++] = b;
    }

    /**
     * Put an array of bytes in the buffer.
     *
     * @param bytes
     *            the byte array
     */
    public void putBytes(byte[] bytes) {
        ensureCapacity(bytes.length);

        System.arraycopy(bytes, 0, this.byteBuffer, this.position, bytes.length);
        this.position += bytes.length;
    }

    /**
     * Put a long as a len encoded value in the buffer.
     *
     * @param length
     *            the value to put in the buffer
     */
    public void putFieldLength(long length) {
        if (length < 251) {
            putByte((byte) length);
        } else if (length < 65536L) {
            ensureCapacity(3);
            putByte((byte) 252);
            putInt16((int) length);
        } else if (length < 16777216L) {
            ensureCapacity(4);
            putByte((byte) 253);
            putInt24((int) length);
        } else {
            ensureCapacity(9);
            putByte((byte) 254);
            putLong(length);
        }
    }

    /**
     * Put an integer in the buffer.
     *
     * @param i
     *            the integer to put in the buffer
     */
    public void putInt16(int i) {
        ensureCapacity(2);

        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
    }

    /**
     * Put a byte array as a len encoded bytes in the buffer.
     *
     * @param bytes
     *            the byte array to put in the buffer
     */
    public void putLenBytes(byte[] bytes) {
        ensureCapacity(9 + bytes.length);

        putFieldLength(bytes.length);
        System.arraycopy(bytes, 0, this.byteBuffer, this.position, bytes.length);
        this.position += bytes.length;
    }

    /**
     * Put a string as a len encoded bytes in the buffer.
     *
     * @param s
     *            the string to put in the buffer
     */
    public void putLenString(String s) {
        if (s == null) {
            putFieldLength(0);
        } else {
            byte[] b = s.getBytes();

            ensureCapacity(9 + b.length);
            putFieldLength(b.length);
            System.arraycopy(b, 0, this.byteBuffer, this.position, b.length);
            this.position += b.length;
        }
    }

    /**
     * Put an integer (MySQL long) in the buffer.
     *
     * @param i
     *            the value to put in the buffer
     */
    public void putInt32(int i) {
        ensureCapacity(4);

        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
    }

    /**
     * Put an unsigned integer (MySQL long) in the buffer.
     *
     * @param i
     *            the value to put in the buffer
     */
    public void putUnsignedInt32(long i) {
        ensureCapacity(4);

        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
    }

    /**
     * Put an integer (MySQL longint) in the buffer
     *
     * @param i
     *            the value to put in the buffer
     */
    public void putInt24(int i) {
        ensureCapacity(3);

        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
    }

    /**
     * Put a long (MySQL longlong) in the buffer.
     *
     * @param i
     *            the value to put in the buffer
     */
    public void putLong(long i) {
        ensureCapacity(8);

        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
        b[this.position++] = (byte) (i >>> 32);
        b[this.position++] = (byte) (i >>> 40);
        b[this.position++] = (byte) (i >>> 48);
        b[this.position++] = (byte) (i >>> 56);
    }

    /**
     * Puts a float in the buffer
     */
    public void putFloat(float f) {
        // TODO: test me
        putInt32(Float.floatToIntBits(f));
    }

    /**
     * Puts a double in the buffer
     */
    public void putDouble(double d) {
        // TODO: test me
        putLong(Double.doubleToLongBits(d));
    }

    /**
     * Put a string in the buffer.
     *
     * @param s
     *            the value to put in the buffer
     */
    public void putString(String s) {
        ensureCapacity((s.length() * 2) + 1);

        System.arraycopy(s.getBytes(), 0, this.byteBuffer, this.position, s.length());
        this.position += s.length();
        this.byteBuffer[this.position++] = 0;
    }

    /**
     * Put a string in the buffer. No null terminated.
     *
     * @param s
     *            the value to put in the buffer
     */
    public void putStringNoNull(String s) {
        ensureCapacity(s.length() * 2);

        System.arraycopy(s.getBytes(), 0, this.byteBuffer, this.position, s.length());
        this.position += s.length();
    }

    /**
     * Converts a time in millis to hours, minutes and seconds and put it in the
     * buffer
     *
     * @param millis
     *            milliseconds since EPOCH
     * @return the actual number of millis written (without the millis info)
     */
    public long putHourMinSec(long millis) {
        long hours = 0, minutes = 0, seconds = 0;
        hours = millis / (1000 * 60 * 60);
        minutes = (millis - (hours * 1000 * 60 * 60)) / (1000 * 60);
        seconds = (millis - (hours * 1000 * 60 * 60) - (minutes * 1000 * 60)) / 1000;

        putByte((byte) hours);
        putByte((byte) minutes);
        putByte((byte) seconds);

        // TODO: millis - millis%1000 ?
        return millis - millis % 1000;
    }

    /**
     * Puts a jdbc {@link java.sql.Time} in the buffer as:<br>
     * Send sign (0 for > EPOCH, 1 for < EPOCH)<br>
     * Then day (always zero for a time)<br>
     * Finally Hour Minutes and Seconds using
     * {@link MySQLPacket#putHourMinSec(long)}<br>
     * Note that MySQL jdbc driver doesn't care about days and millis, so it is
     * most probably unnecessary to send them...
     *
     * @param t
     *            the Time object to write
     */
    public void putTime(Time t) {

        // guess sign and normalize millis
        long millis = t.getTime();
        if (millis < 0) {
            putByte((byte) 1);
            millis = -millis;
        } else
            putByte((byte) 0);

        // Send a fake day
        putInt32(0);

        // Send the 3 bytes HHMMSS
        putHourMinSec(millis);

        // Don't send millis
    }

    /**
     * Puts a {@link Date} to the buffer as:<br>
     * two bytes year one byte month one byte day
     *
     * @param d
     *            jdbc date to write
     * @return the rounded date as milliseconds actually put (without hh mm ss
     *         ms information)
     */
    public long putDate(Date d) {
        Calendar fullDate = new GregorianCalendar();
        fullDate.clear();
        fullDate.setTimeInMillis(d.getTime());
        // MYO-100: We need to return the remaining millis for the date only,
        // not for its HHMMSSMS
        Calendar yymmddOnly = new GregorianCalendar(fullDate.get(Calendar.YEAR), fullDate.get(Calendar.MONTH), fullDate.get(Calendar.DAY_OF_MONTH));
        // Note: Supported range for MySQL date is '1000-01-01' to '9999-12-31'.
        // So we don't care about era field (BC/AD)
        putInt16(yymmddOnly.get(Calendar.YEAR));
        // 1-based (ie. 1 for January)
        putByte((byte) (1 + yymmddOnly.get(Calendar.MONTH)));
        // 1-based (ie. 1 for 1st of the month)
        putByte((byte) yymmddOnly.get(Calendar.DAY_OF_MONTH));
        return yymmddOnly.getTimeInMillis();
    }

    /**
     * Rewinds data buffer pointer to the first data byte.
     */
    public void reset() {
        this.position = HEADER_LENGTH;
    }

    /**
     * Write out the content of the buffer to the output stream.
     *
     * @param out
     *            the output stream
     * @throws IOException
     *             if an error happens when writting to the output stream
     */
    public void write(OutputStream out) throws IOException {
        int len = this.position - HEADER_LENGTH;

        // TODO handle packets bigger than 16 MB
        // for now only we return an error message
        if (len >= 256 * 256 * 256) {
            String message = "Trying to send packet of size " + len + ", packets bigger than 16 MB are not supported yet!";
            logger.error(message);
            throw new IOException(message);
        }

        byte[] b = this.byteBuffer;
        b[0] = (byte) (len & 0xff);
        b[1] = (byte) (len >>> 8);
        b[2] = (byte) (len >>> 16);

        out.write(b, 0, this.position);
    }

    /**
     * Ensure that there are additionalData bytes available in the buffer. If
     * not realocate the buffer.
     *
     * @param additionalData
     *            the number of bytes what need to be available
     */
    private void ensureCapacity(int additionalData) {
        if ((this.position + additionalData) > this.byteBuffer.length) {
            int newLength = (int) (this.byteBuffer.length * 1.25);

            if (newLength < (this.byteBuffer.length + additionalData)) {
                newLength = this.byteBuffer.length + (int) (additionalData * 1.25);
            }

            if (newLength < this.byteBuffer.length) {
                newLength = this.byteBuffer.length + additionalData;
            }

            byte[] newBytes = new byte[newLength];

            System.arraycopy(this.byteBuffer, 0, newBytes, 0, this.byteBuffer.length);
            this.byteBuffer = newBytes;
        }
    }

    /**
     * Returns the len encoded value as a long from buffer.
     *
     * @return the len encoded value as a long from buffer
     */
    public long readFieldLength() {
        int sw = this.byteBuffer[this.position++] & 0xff;

        switch (sw) {
        case 251:
            return NULL_LENGTH;

        case 252:
            return getUnsignedShort();

        case 253:
            return getUnsignedInt24();

        case 254:
            long l = getLong();
            if (l > Integer.MAX_VALUE)
                logger.warn("field length bug here!");

            return l;

        default:
            return sw;
        }
    }

    /**
     * Retrieves the input stream that created this packet
     *
     * @return the input stream used to read the current packet
     */
    public InputStream getInputStream() {
        return inputStream;
    }

    /**
     * Retain creation input stream for further re-reads
     *
     * @param inputStream
     *            the input stream that created this packet
     */
    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    /**
     * Provided this packet is a large packet > 16M, reads the remaining packets
     * and and adds their data to this packet data. The result will consist in
     * one large packet with all data in. Note that the new packet's header will
     * be invalid
     */
    public void readRemainingPackets() {
        if (getDataLength() != MAX_LENGTH) {
            logger.error("readRemainingPackets() is only relevant for large packets!");
            return;
        }
        // Read all packets into an array list
        ArrayList<MySQLPacket> nextPackets = new ArrayList<MySQLPacket>();
        MySQLPacket nextPacket = readPacket(getInputStream());
        while (nextPacket.getDataLength() == MAX_LENGTH) {
            nextPackets.add(nextPacket);
            nextPacket = readPacket(getInputStream());
        }
        nextPackets.add(nextPacket);
        // get the new size
        int newSize = getDataLength();
        for (MySQLPacket packet : nextPackets) {
            newSize += packet.getDataLength();
        }
        // create a new large buffer
        byte[] newBytes = new byte[newSize + HEADER_LENGTH];
        // copy this packet data into the new buffer
        System.arraycopy(byteBuffer, 0, newBytes, 0, getDataLength() + HEADER_LENGTH);
        int cursor = getDataLength() + HEADER_LENGTH;
        // copy next packets data into at the end of the new buffer
        for (MySQLPacket packet : nextPackets) {
            System.arraycopy(packet.getByteBuffer(), HEADER_LENGTH, newBytes, cursor, packet.getDataLength());
            cursor += packet.getDataLength();
        }
        byteBuffer = newBytes;
        dataLength = newSize;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("Packet #").append(this.getPacketNumber());
        sb.append(" size=").append(this.getDataLength());
        sb.append(" pos=").append(this.getPacketPosition());
        sb.append(" data=");
        if (getDataLength() < 1024) {
            sb.append(Utils.byteArrayToHexString(byteBuffer));
            sb.append(" text data=");
            for (int i = 0; i < this.byteBuffer.length; i++) {
                sb.append((char) this.byteBuffer[i]);
                sb.append(" ");
            }
        } else {
            for (int i = 0; i < 1024; i++) {
                sb.append(Utils.byteToHexString(this.byteBuffer[i]));
                sb.append(' '); // separate bytes with a space
            }
            sb.append(" ... - text data=");
            for (int i = 0; i < 1024; i++) {
                if (Character.isValidCodePoint(this.byteBuffer[i])) {
                    sb.append((char) this.byteBuffer[i]);
                    sb.append(' ');
                }
            }
            sb.append("... [data above 1Kb not displayed]");
        }

        return sb.toString();
    }
}
