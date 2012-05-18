/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

public class Column implements Serializable {
    private static final long serialVersionUID = 1L;
    String name;
    int type; // Type assignment from
    // java.sql.Types
    boolean signed;
    long length;
    boolean notNull; // Is the column a NOT
    // NULL column
    Serializable value;
    int valueInputStreamLength;
    private int position;
    private boolean blob;
    private String typeDescription;

    public Column(String name, int type) {
        this(name, type, false);
    }

    public Column(String name, int type, boolean isNotNull) {
        this(name, type, 0, isNotNull);
    }

    public Column(String name, int type, boolean isNotNull, boolean isSigned) {
        this(name, type, 0, isNotNull);
        signed = isSigned;
    }

    public Column(String name, int type, int length) {
        this(name, type, length, false);
    }

    public Column(String name, int type, int length, boolean isNotNull) {
        this(name, type, length, isNotNull, null);
    }

    public Column(String name, int type, long colLength, boolean isNotNull, Serializable value) {
        this.name = name;
        this.type = type;
        this.length = colLength;
        this.notNull = isNotNull;
        this.value = value;
        this.signed = true;
        this.blob = false;
    }

    public String getName() {
        return this.name;
    }

    public int getType() {
        return this.type;
    }

    public long getLength() {
        return this.length;
    }

    /**
     * Is the column a NOT NULL column
     */
    public boolean isNotNull() {
        return this.notNull;
    }

    /**
     * Is the current value of the column NULL
     */
    public boolean isNull() {
        return (this.value == null);
    }

    public void Dump() {
        System.out.format("%s\n", name);
    }

    public void setValue(Serializable value) {
        this.value = value;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setValue(short valueShort) {
        this.value = new Short(valueShort);
    }

    public void setValue(int valueInt) {
        this.value = new Integer(valueInt);
    }

    public void setValue(long valueLong) {
        this.value = new Long(valueLong);
    }

    public void setValue(String valueString) {
        value = valueString;
    }

    public void setValue(InputStream valueInputStream, int valueInputStreamLength) {
        byte[] byteArray = new byte[valueInputStreamLength];
        try {
            valueInputStream.read(byteArray, 0, valueInputStreamLength);
            this.value = byteArray;
        } catch (IOException e) {
            throw new RuntimeException("Unable to read input stream into column value: stream length=" + valueInputStreamLength, e);
        }
    }

    public void setValueNull() {
        this.value = null;
    }

    public Serializable getValue() {
        return this.value;
    }

    public int getValueInt() {
        return (Integer) this.value;
    }

    public long getValueLong() {
        return (Long) this.value;
    }

    public String getValueString() {
        return this.value.toString();
    }

    public void setPosition(int columnIdx) {
        position = columnIdx;
    }

    public int getPosition() {
        return position;
    }

    public boolean isSigned() {
        return signed;
    }

    public boolean isBlob() {
        return blob;
    }

    public void setBlob(boolean blob) {
        this.blob = blob;
    }

    public String getTypeDescription() {
        return typeDescription;
    }

    public void setTypeDescription(String typeDescription) {
        this.typeDescription = typeDescription;
    }

}