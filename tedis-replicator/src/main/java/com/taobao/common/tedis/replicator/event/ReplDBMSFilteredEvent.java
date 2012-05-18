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

public class ReplDBMSFilteredEvent extends ReplDBMSEvent {
    private long seqnoEnd = -1;
    private short fragnoEnd = -1;

    public ReplDBMSFilteredEvent(String lastFilteredId, Long firstFilteredSeqno, Long lastFilteredSeqno, Short lastFragno) {
        super(firstFilteredSeqno, new DBMSEvent(lastFilteredId));
        this.seqnoEnd = lastFilteredSeqno;
        this.fragnoEnd = lastFragno;
    }

    public ReplDBMSFilteredEvent(Long firstFilteredSeqno, Short firstFilteredFragno, Long lastFilteredSeqno, Short lastFilteredFragno, boolean lastFrag, String eventId, String sourceId,
            Timestamp timestamp) {
        super(firstFilteredSeqno, new DBMSEvent(eventId, null, timestamp));
        this.seqnoEnd = lastFilteredSeqno;
        this.fragno = firstFilteredFragno;
        this.fragnoEnd = lastFilteredFragno;
        this.lastFrag = lastFrag;
        this.sourceId = sourceId;
    }

    public ReplDBMSFilteredEvent(ReplDBMSHeader firstFilteredEvent, ReplDBMSHeader lastFilteredEvent) {
        super(firstFilteredEvent.getSeqno(), new DBMSEvent(lastFilteredEvent.getEventId()));
        this.seqnoEnd = lastFilteredEvent.getSeqno();
        this.fragno = firstFilteredEvent.getFragno();
        this.fragnoEnd = lastFilteredEvent.getFragno();
        this.lastFrag = lastFilteredEvent.getLastFrag();
        this.sourceId = firstFilteredEvent.getSourceId();
    }

    private static final long serialVersionUID = 1L;

    public long getSeqnoEnd() {
        return seqnoEnd;
    }

    public void updateCommitSeqno() {
        this.seqno = this.seqnoEnd;
    }

    public short getFragnoEnd() {
        return fragnoEnd;
    }

}
