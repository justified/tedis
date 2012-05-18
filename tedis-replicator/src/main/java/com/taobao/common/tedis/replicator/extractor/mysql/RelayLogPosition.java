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

public class RelayLogPosition {
    protected File curFile;
    protected long curOffset;

    public RelayLogPosition() {
    }

    public synchronized void setPosition(File file, long offset) {
        this.curFile = file;
        this.curOffset = offset;
    }

    public synchronized void setOffset(int offset) {
        this.curOffset = offset;
    }

    public synchronized File getFile() {
        return curFile;
    }

    public synchronized long getOffset() {
        return curOffset;
    }

    public synchronized RelayLogPosition clone() {
        RelayLogPosition clone = new RelayLogPosition();
        clone.setPosition(curFile, curOffset);
        return clone;
    }

    public synchronized boolean hasReached(String fileName, long offset) {
        if (curFile == null)
            return false;
        else if (curFile.getName().compareTo(fileName) < 0) {
            // Our file name is greater, position has not been reached.
            return false;
        } else if (curFile.getName().compareTo(fileName) == 0) {
            // Our file name is the same, we must compare the offset.
            if (offset > curOffset)
                return false;
            else
                return true;
        } else {
            // Our file name is less. We have reached the position.
            return true;
        }
    }

    public synchronized String toString() {
        if (curFile == null)
            return "";
        else
            return curFile.getName() + ":" + curOffset;
    }
}