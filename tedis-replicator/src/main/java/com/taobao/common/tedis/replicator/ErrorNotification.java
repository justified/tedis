/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator;

import com.taobao.common.tedis.replicator.statemachine.Event;
import com.taobao.common.tedis.replicator.statemachine.event.OutOfBandEvent;

/**
 * 严重的错误，会导致程序终止；被标记为OutOfBandEvent，所以肯定会执行。
 */
public class ErrorNotification extends Event implements OutOfBandEvent {
    private final String userMessage;
    private final long seqno;
    private final String eventId;

    public ErrorNotification(String userMessage, Throwable e) {
        super(e);
        this.userMessage = userMessage;
        this.seqno = -1;
        this.eventId = null;
    }

    public ErrorNotification(String userMessage, long seqno, String eventId, Throwable e) {
        super(e);
        this.userMessage = userMessage;
        this.seqno = seqno;
        this.eventId = eventId;
    }

    /**
     * Returns the original source of the error.
     */
    public Throwable getThrowable() {
        return (Throwable) getData();
    }

    /**
     * Returns a message suitable for users.
     */
    public String getUserMessage() {
        return userMessage;
    }

    /**
     * Returns the log sequence number associated with failure or -1 if there is
     * no such number.
     */
    public long getSeqno() {
        return seqno;
    }

    /**
     * Returns the native event ID associated with failure or null if there is
     * no such ID.
     */
    public String getEventId() {
        return eventId;
    }
}