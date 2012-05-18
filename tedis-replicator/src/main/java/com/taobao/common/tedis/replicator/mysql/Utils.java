/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.mysql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

public class Utils {
    /** Logger for this class */
    private static final Logger logger = Logger.getLogger(Utils.class);

    /** All hexadecimal characters */
    final static String hexChars[] = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F" };

    /**
     * Generate a random string. This is used for password encryption.
     *
     * @return the generated random String
     */
    public static String generateRandomString(int count) {
        Random random = new Random();
        StringBuffer buffer = new StringBuffer();

        while (count-- != 0) {
            // a random number in the 32...127 range
            char ch = (char) (random.nextInt(96) + 32);
            buffer.append(ch);
        }

        return buffer.toString();
    }

    /**
     * Trims white spaces and remove quotes from the string.
     *
     * @param s
     *            the string
     * @return a string without quotes
     */
    public static String removeQuotes(String s) {
        if (s == null) {
            return null;
        }

        String trimmed = s.trim();

        if (trimmed.length() == 0) {
            return trimmed;
        }

        int i = nextNonQuoteIndex(trimmed, 0, true);

        int j = nextNonQuoteIndex(trimmed, trimmed.length() - 1, false);

        return trimmed.substring(i, j + 1);
    }

    /**
     * Computes the index of the next character in the given string that is not
     * a quote, starting from given index and going either forward or backward
     *
     * @param trimmed
     *            the string to analyze
     * @param i
     *            start index
     * @param forward
     *            whether to increase index (true) or to decrease it (false)
     * @return the index of the next non quote character
     */
    private static int nextNonQuoteIndex(String trimmed, int i, boolean forward) {
        while (trimmed.charAt(i) == '\u0022' || trimmed.charAt(i) == '\'' || trimmed.charAt(i) == '\u0060' || trimmed.charAt(i) == '\u00B4' || trimmed.charAt(i) == '\u2018'
                || trimmed.charAt(i) == '\u2019' || trimmed.charAt(i) == '\u201C' || trimmed.charAt(i) == '\u201D') {
            if (forward)
                i++;
            else
                i--;
        }
        return i;
    }

    /**
     * Replace parameters $1, $2, ... with '?'.
     *
     * @param statement
     *            the statement
     * @return a string where parameters get replaced with question mark
     */
    public static String replaceParametersWithQuestionMarks(String statement) {
        /* TODO handle parameters surrounded with quote */

        if (statement == null) {
            return null;
        }

        String result = "";
        int i = 0;
        int len = statement.length();
        char last = '\0';

        while (i < len) {
            char c = statement.charAt(i);
            if (c == '$') {
                last = c;
                i++;
                continue;
            }
            if ((last == '$') && (c >= '0') && (c <= '9')) {
                i++;
                while ((i < len) && (Character.isDigit(statement.charAt(i)))) {
                    i++;
                }
                last = '\0';
                c = '?';
                i--;
            }
            result += c;
            i++;
        }
        return result;
    }

    /**
     * Turns 16-byte stream into a human-readable 32-byte hex string This code
     * was copied from the PostgreSQL JDBC driver code (MD5Digest.java)
     *
     * @param bytes
     *            bytes to convert
     * @param hex
     *            the converted hex bytes
     * @param offset
     *            from where to start the conversion
     */
    public static void bytesToHex(byte[] bytes, byte[] hex, int offset) {
        final char lookup[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

        int i, c, j, pos = offset;

        for (i = 0; i < 16; i++) {
            c = bytes[i] & 0xFF;
            j = c >> 4;
            hex[pos++] = (byte) lookup[j];
            j = (c & 0xF);
            hex[pos++] = (byte) lookup[j];
        }
    }

    /**
     * Converts a byte to readable hexadecimal format in a string<br>
     * This code was originally taken from Jeff Boyle's article on devX.com
     *
     * @param in
     *            byte[] buffer to convert to string format
     * @return a String containing the hex values corresponding to the input
     *         byte, all attached
     */
    public static String byteToHexString(byte in) {
        StringBuffer out = new StringBuffer(2);
        // Strip off high nibble
        byte ch = (byte) (in & 0xF0);
        // shift the bits down
        ch = (byte) (ch >>> 4);
        ch = (byte) (ch & 0x0F);
        // must do this is high order bit is on!
        out.append(hexChars[(int) ch]); // convert the nibble to a String
        // Character

        ch = (byte) (in & 0x0F); // Strip off low nibble

        out.append(hexChars[(int) ch]); // convert the nibble to a String
        // Character

        return out.toString();
    }

    /**
     * Converts the given byte array into a readable hexa-decimal formatted
     * string, starting from given offset<br>
     * This code was originally taken from Jeff Boyle's article on devX.com
     *
     * @return a String containing the hex values corresponding to the input
     *         bytes, bytes separated by a space
     * @param in
     *            byte[] buffer to convert to string format
     * @param offset
     *            where to start the conversion from
     */
    public static String byteArrayToHexString(byte in[], int offset) {

        if (in == null || in.length <= 0 || offset >= in.length)
            return null;

        StringBuffer out = new StringBuffer(in.length * 3);

        for (int i = offset; i < in.length; i++) {
            out.append(byteToHexString(in[i]));
            out.append(' '); // separate bytes with a space
        }
        return out.toString();
    }

    /**
     * Converts the given byte array into a readable hexa-decimal formatted
     * string
     *
     * @return a String containing the hex values corresponding to the input
     *         bytes, bytes separated by a space
     * @param in
     *            byte[] buffer to convert to string format
     */
    public static String byteArrayToHexString(byte in[]) {
        return byteArrayToHexString(in, 0);
    }

    /**
     * Converts the given array of bytes into a Java ArrayList
     *
     * @param byteArray
     *            array of bytes to convert
     * @return an ArrayList containing the given byteArray data
     */
    public static ArrayList<Byte> byteArrayToArrayList(byte[] byteArray) {
        return byteArrayToArrayList(byteArray, 0);
    }

    /**
     * Converts the given array of bytes into a Java ArrayList starting from the
     * given offset
     *
     * @param byteArray
     *            array of bytes to convert
     * @param offset
     *            where to start conversion from
     * @return an ArrayList containing the given byteArray data
     */
    public static ArrayList<Byte> byteArrayToArrayList(byte[] byteArray, int offset) {
        ArrayList<Byte> list = new ArrayList<Byte>(byteArray.length);
        for (int i = offset; i < byteArray.length; i++) {
            list.add(Byte.valueOf(byteArray[i]));
        }
        return list;
    }

    /**
     * Removes comments (between slash-star and star-slash plus lines beginning
     * with double slash) comments from a sql statement and returns the
     * resulting statement.
     *
     * @param sqlStatement
     *            a sql statement in which we want to remove comments
     * @return the SQL statement without comments
     */
    public static String removeComments(String sqlStatement) {
        StringBuffer result = new StringBuffer();
        int index = sqlStatement.indexOf("/*");
        int lastIndex = 0;

        while (index >= 0) {
            result.append(sqlStatement.substring(lastIndex, index));

            // Comment marker found... Look for the end marker
            int nextIndex = sqlStatement.indexOf("*/", index + 2);
            if (nextIndex == -1)
                return sqlStatement;

            lastIndex = nextIndex + 2;
            index = sqlStatement.indexOf("*/", lastIndex);
        }
        result.append(sqlStatement.substring(lastIndex));

        // Do another round line by line to remove line-comments
        BufferedReader reader = new BufferedReader(new StringReader(result.toString().trim()));
        result = new StringBuffer();
        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                int firstCommentIdx = line.indexOf("--");
                int doubleSlashCommentIdx = line.indexOf("//");
                if (firstCommentIdx == -1 || (doubleSlashCommentIdx != -1 && doubleSlashCommentIdx < firstCommentIdx)) {
                    firstCommentIdx = doubleSlashCommentIdx;
                }
                String toAppend = null;
                if (firstCommentIdx == -1)
                    toAppend = line;
                else
                    toAppend = line.substring(0, firstCommentIdx);
                toAppend = toAppend.trim();
                if (!"".equals(toAppend)) {
                    result.append(line);
                    result.append("\n");
                }
            }
        } catch (IOException neverHappens) {
        }
        return result.toString().trim();
    }

    /**
     * Applies the given mask to the given IP
     *
     * @param ip
     *            the IPV4 address to apply mask to
     * @param mask
     *            the IPV4 mask to apply to the given IP
     * @return an InetAddress representing the given masked IP
     */
    public static InetAddress applyMask(String ip, String mask) {
        byte[] rawIP = null;
        byte[] rawMask = null;
        try {
            rawIP = InetAddress.getByName(ip).getAddress();
            rawMask = InetAddress.getByName(mask).getAddress();
            if (rawIP.length != rawMask.length) {
                logger.error("IP " + ip + " and mask " + mask + " use different formats");
                return null;
            }
            byte[] maskedAddressBytes = new byte[rawIP.length];
            for (int i = 0; i < rawIP.length; i++) {
                byte currentAddressByte = rawIP[i];
                byte currentMaskByte = rawMask[i];
                maskedAddressBytes[i] = (byte) (currentAddressByte & currentMaskByte);
            }

            return InetAddress.getByAddress(maskedAddressBytes);
        } catch (UnknownHostException uhe) {
            logger.debug("Caught UnknownHostException while applying mask " + mask + " to IP " + ip, uhe);
            return null;
        }

    }

    /**
     * Transforms a CIDR formatted mask into a regular network mask
     *
     * @param cidrMask
     *            mask in CIDR format (24, 32, etc.)
     * @return a network mask like 255.255.255.0
     */
    public static String cidrMaskToNetMask(String cidrMask) {
        if (cidrMask == null) {
            return null;
        }
        // Get the integer value of the mask
        int cidrMaskValue = 0;
        try {
            cidrMaskValue = Integer.parseInt(cidrMask);
        } catch (NumberFormatException e) {
            return null;
        }
        int cidrMaskFull = 0xffffffff << (32 - cidrMaskValue);
        int cidrMaskBits1 = cidrMaskFull >> 24 & 0xff;
        int cidrMaskBits2 = cidrMaskFull >> 16 & 0xff;
        int cidrMaskBits3 = cidrMaskFull >> 8 & 0xff;
        int cidrMaskBits4 = cidrMaskFull >> 0 & 0xff;

        StringBuffer netMaskBuf = new StringBuffer();
        netMaskBuf.append(cidrMaskBits1);
        netMaskBuf.append('.');
        netMaskBuf.append(cidrMaskBits2);
        netMaskBuf.append('.');
        netMaskBuf.append(cidrMaskBits3);
        netMaskBuf.append('.');
        netMaskBuf.append(cidrMaskBits4);

        return netMaskBuf.toString();
    }

    /**
     * Tells whether the given IPV4 address is in the given CIDR ip range
     *
     * @param ip
     *            the IP address to compare, in ipv4 format
     * @param ipRange
     *            the network+mask to test, in CIDR format
     * @return true if the given IP is in the given range, false otherwise
     */
    public static boolean isInRange(String ip, String ipRange) {
        if (ip == null || ipRange == null)
            return false;

        // separate network part from mask part
        String[] cidrString = ipRange.split("/");
        if (cidrString.length == 0)
            return false;
        String network = cidrString[0];
        String cidrMask = "24";
        // if there is something after '/', that our mask, otherwise, that's a
        // single address
        if (cidrString.length > 1) {
            cidrMask = cidrString[1];
        }

        // Get a regular network mask to apply to the address
        String netMask = cidrMaskToNetMask(cidrMask);
        // Apply it
        InetAddress maskedIP = applyMask(ip, netMask);
        InetAddress maskedNetwork = applyMask(network, netMask);
        if (maskedIP == null || maskedNetwork == null)
            // malformed addresses
            return false;
        return maskedIP.equals(maskedNetwork);
    }

    /**
     * Tells whether the given IP belongs to the given list of CIDR addresses.
     * Null IPs are always denied, null authorizedIPs allow any host (except
     * null ones)
     *
     * @param ip
     *            the IP address to test
     * @param authorizedIPs
     *            a list of CIDR formatted network/mask. Null for authorizing
     *            any host
     * @return true if the given IP is part of one or more given IP ranges
     */
    public static boolean isAuthorizedIP(String ip, List<String> authorizedIPs) {
        if (ip == null)
            return false;
        if (authorizedIPs == null) {
            return true;
        }
        for (String ipRange : authorizedIPs) {
            if (ipRange != null)
                if (isInRange(ip, ipRange))
                    return true;
        }
        return false;
    }
}
