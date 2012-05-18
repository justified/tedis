/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.util;

import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;

/**
 * Implements a WatchPredicate to identify that a particular sequence number has
 * been reached. This returns true for any sequence number equal to
 * <em>or higher than</em> the number we are seeking.
 */
public class SeqnoWatchPredicate implements WatchPredicate<ReplDBMSHeader> {
    private final long seqno;

    public SeqnoWatchPredicate(long seqno) {
        this.seqno = seqno;
    }

    public boolean match(ReplDBMSHeader event) {
        if (event == null)
            return false;
        else if (event.getSeqno() > seqno)
            return true;
        else if (event.getSeqno() == seqno && event.getLastFrag())
            return true;
        else
            return false;
    }

    public long getSeqno() {
        return seqno;
    }
}