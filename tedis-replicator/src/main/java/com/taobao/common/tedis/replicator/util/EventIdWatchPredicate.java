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

public class EventIdWatchPredicate implements WatchPredicate<ReplDBMSHeader> {
    private final String eventId;

    public EventIdWatchPredicate(String eventId) {
        this.eventId = eventId;
    }

    public boolean match(ReplDBMSHeader event) {
        if (event == null)
            return false;
        else if (event.getEventId() == null)
            return false;
        else if (event.getEventId().compareTo(eventId) < 0)
            return false;
        else
            return true;
    }

    public String toString() {
        return this.getClass().getName() + " eventId=" + eventId;
    }
}