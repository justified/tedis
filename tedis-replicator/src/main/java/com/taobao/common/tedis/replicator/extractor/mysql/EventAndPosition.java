/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import com.taobao.common.tedis.replicator.extractor.ExtractorException;

/**
 * Store binlog position and log event extracted from.
 *
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 */
public class EventAndPosition {
    private final BinlogPosition position;

    protected LogEvent event;

    // native thrown exception
    protected ExtractorException error = null;

    public EventAndPosition(BinlogPosition position) {
        this.position = position;
    }

    public EventAndPosition(BinlogPosition position, LogEvent event) {
        this.position = position;
        this.event = event;
    }

    public EventAndPosition(BinlogPosition position, ExtractorException error) {
        this.position = position;
        this.error = error;
    }

    public final BinlogPosition getPosition() {
        return position;
    }

    public LogEvent getEvent() throws InterruptedException, ExtractorException {
        if (error != null)
            throw error;

        return event;
    }
}
