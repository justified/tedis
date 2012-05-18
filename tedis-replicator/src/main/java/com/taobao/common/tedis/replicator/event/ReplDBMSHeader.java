/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.event;

import java.sql.Timestamp;

public interface ReplDBMSHeader {
    public long getSeqno();

    public short getFragno();

    public boolean getLastFrag();

    public String getSourceId();

    public long getEpochNumber();

    public String getEventId();

    public Timestamp getExtractedTstamp();
}