/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.data;

import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;

import com.taobao.common.tedis.replicator.event.ReplOption;
import com.taobao.common.tedis.replicator.event.ReplOptionParams;

public class StatementData extends DBMSData {
    private static final long serialVersionUID = 1L;

    public static final String CREATE_OR_DROP_DB = "createOrDropDB";

    private String defaultSchema;
    private Long timestamp;

    private String query;
    private byte[] queryAsBytes;

    // Internal buffer used to return string values. This value is not
    // serialized.
    private transient String queryAsBytesTranslated;

    // Transient SQL parsing metata stored here to avoid later reparsing.
    private transient Object metadata;

    private List<ReplOption> options = null;

    private int errorCode;

    public StatementData(String query) {
        super();
        this.defaultSchema = null;
        this.timestamp = null;
        this.query = query;
    }

    public StatementData(String query, Long timestamp, String defaultSchema) {
        this.defaultSchema = defaultSchema;
        this.timestamp = timestamp;
        this.query = query;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getQuery() {
        if (this.queryAsBytes == null)
            return query;
        else {
            if (this.queryAsBytesTranslated == null) {
                // If we need a string and don't have it, we use a temporary
                // translation buffer. If we know the byte character set, we
                // translate faithfully. If we don't know or the encoding is
                // unknown, we fall back to the platform character set.
                String charsetName = getOption(ReplOptionParams.JAVA_CHARSET_NAME);
                if (charsetName == null)
                    queryAsBytesTranslated = new String(queryAsBytes);
                else {
                    try {
                        queryAsBytesTranslated = new String(queryAsBytes, charsetName);
                    } catch (UnsupportedEncodingException e) {
                        queryAsBytesTranslated = new String(queryAsBytes);
                    }
                }
            }
            return queryAsBytesTranslated;
        }
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setQuery(String query) {
        this.query = query;
        this.queryAsBytes = null;
    }

    public void setQuery(byte[] query) {
        this.queryAsBytes = query;
        this.query = null;
    }

    public void appendToQuery(String buffer) {
        if (this.queryAsBytes == null)
            query = query + buffer;
        else {
            String charset = getCharset();
            byte[] appendBuffer;
            if (charset == null)
                appendBuffer = buffer.getBytes();
            else {
                try {
                    appendBuffer = buffer.getBytes(charset);
                } catch (UnsupportedEncodingException e) {
                    appendBuffer = buffer.getBytes();
                }
            }
            byte[] buf = new byte[queryAsBytes.length + appendBuffer.length];
            System.arraycopy(queryAsBytes, 0, buf, 0, queryAsBytes.length);
            System.arraycopy(appendBuffer, 0, buf, queryAsBytes.length, appendBuffer.length);
            queryAsBytes = buf;
            queryAsBytesTranslated = null;
        }
    }

    public void addOption(String name, String value) {
        if (options == null)
            options = new LinkedList<ReplOption>();
        options.add(new ReplOption(name, value));
    }

    public List<ReplOption> getOptions() {
        return options;
    }

    /**
     * Returns an option value or null if not found.
     *
     * @param name
     *            Option name
     */
    public String getOption(String name) {
        if (options == null)
            return null;
        else {
            for (ReplOption replOption : options) {
                if (name.equals(replOption.getOptionName()))
                    return replOption.getOptionValue();
            }
            return null;
        }
    }

    /**
     * Returns the Java character set name of the statement as represented in
     * bytes or null if not known.
     */
    public String getCharset() {
        return getOption(ReplOptionParams.JAVA_CHARSET_NAME);
    }

    /**
     * Sets the character set name for this statement as represented in bytes.
     *
     * @param charset
     *            Java character set name
     */
    public void setCharset(String charset) {
        addOption(ReplOptionParams.JAVA_CHARSET_NAME, charset);
    }

    @Override
    public String toString() {
        String toStringValue = getQuery();
        if (toStringValue.length() > 1000)
            return toStringValue.substring(0, 999);
        else
            return toStringValue;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public byte[] getQueryAsBytes() {
        return queryAsBytes;
    }

    public Object getParsingMetadata() {
        return this.metadata;
    }

    public void setParsingMetadata(Object o) {
        this.metadata = o;
    }
}
