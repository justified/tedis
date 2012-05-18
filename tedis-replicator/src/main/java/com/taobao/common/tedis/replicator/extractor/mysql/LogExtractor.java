/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.extractor.ExtractorException;

public interface LogExtractor {
    public EventAndPosition extractLogEvent() throws ExtractorException, InterruptedException;

    public void setBinlogFile(String binlogFileName) throws MySQLExtractException, InterruptedException;

    public void nextBinlogFile(BinlogPosition position) throws ExtractorException, InterruptedException;

    public void initBinlogPosition(String binlogFileIndex, long binlogOffset) throws ExtractorException;

    public BinlogPosition getResourcePosition() throws ExtractorException, InterruptedException;

    void setUsingBytesForString(boolean useBytes);
    
    void release() throws ReplicatorException, InterruptedException;

}