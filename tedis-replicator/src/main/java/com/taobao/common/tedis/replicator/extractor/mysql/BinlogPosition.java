/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

public class BinlogPosition implements Cloneable {
    private String directory;
    private String fileName;

    private long position;

    public BinlogPosition(long position, String fileName, String directory) {
        this.directory = directory;
        this.fileName = fileName;
        this.position = position;
    }

    public BinlogPosition clone() {
        try {
            return (BinlogPosition) super.clone();
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }

    public void setPosition(long newPosition) {
        position = newPosition;
    }

    public long getPosition() {
        return position;
    }

    public void setFileName(String fileName) {
        if (!this.fileName.equals(fileName)) {
            this.fileName = fileName;
            this.position = 0;
        }
    }

    public String getFileName() {
        return (fileName);
    }

    public String getDirectory() {
        return (directory);
    }

    public String toString() {
        return fileName + " (" + getPosition() + ")";
    }
}
