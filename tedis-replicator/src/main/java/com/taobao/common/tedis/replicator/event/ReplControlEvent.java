/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.event;

public class ReplControlEvent extends ReplEvent {
    private static final long serialVersionUID = -4085859758593913496L;

    public static final int STOP = 1;

    public static final int SYNC = 2;

    private final int eventType;
    private final long seqno;
    private final ReplDBMSHeader header;

    public ReplControlEvent(int eventType, long seqno, ReplDBMSHeader header) {
        this.eventType = eventType;
        this.seqno = seqno;
        this.header = header;
    }

    public int getEventType() {
        return eventType;
    }

    public ReplDBMSHeader getHeader() {
        return header;
    }

    public long getSeqno() {
        return seqno;
    }
}