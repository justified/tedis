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

public class ReplDBMSHeaderData implements ReplDBMSHeader {
    private final long seqno;
    private final short fragno;
    private final boolean lastFrag;
    private final String sourceId;
    private final long epochNumber;
    private final String eventId;
    private final Timestamp extractedTstamp;

    public ReplDBMSHeaderData(long seqno, short fragno, boolean lastFrag, String sourceId, long epochNumber, String eventId, Timestamp extractedTstamp) {
        this.seqno = seqno;
        this.fragno = fragno;
        this.lastFrag = lastFrag;
        this.sourceId = sourceId;
        this.epochNumber = epochNumber;
        this.eventId = eventId;
        this.extractedTstamp = extractedTstamp;
    }

    public ReplDBMSHeaderData(ReplDBMSHeader event) {
        this.seqno = event.getSeqno();
        this.fragno = event.getFragno();
        this.lastFrag = event.getLastFrag();
        this.sourceId = event.getSourceId();
        this.epochNumber = event.getEpochNumber();
        this.eventId = event.getEventId();
        this.extractedTstamp = event.getExtractedTstamp();
    }

    public long getSeqno() {
        return seqno;
    }

    public String getEventId() {
        return eventId;
    }

    public long getEpochNumber() {
        return epochNumber;
    }

    public short getFragno() {
        return fragno;
    }

    public boolean getLastFrag() {
        return lastFrag;
    }

    public String getSourceId() {
        return sourceId;
    }

    public Timestamp getExtractedTstamp() {
        return extractedTstamp;
    }
}