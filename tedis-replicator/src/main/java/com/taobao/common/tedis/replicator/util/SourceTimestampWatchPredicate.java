/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.util;

import java.sql.Timestamp;

import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;

/**
 * Implements a WatchPredicate to identify that a particular sequence number has
 * been reached. This returns true for any sequence number equal to
 * <em>or higher than</em> the number we are seeking.
 */
public class SourceTimestampWatchPredicate implements WatchPredicate<ReplDBMSHeader> {
    private final Timestamp timestamp;

    public SourceTimestampWatchPredicate(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public boolean match(ReplDBMSHeader event) {
        if (event == null)
            return false;
        else {
            Timestamp sourceTimestamp = event.getExtractedTstamp();
            if (sourceTimestamp == null || sourceTimestamp.before(timestamp))
                return false;
            else
                return true;
        }
    }
}