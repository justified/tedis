/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.taobao.common.tedis.replicator.extractor.ExtractorException;

public final class EventAndPositionFuture extends EventAndPosition {
    private final Future<LogEvent> future;

    public EventAndPositionFuture(BinlogPosition position, Future<LogEvent> future) {
        super(position);

        this.future = future;
    }

    private final LogEvent fetchEvent() throws InterruptedException, ExtractorException {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause == null)
                error = new ExtractorException("Execution exception", e);
            if (cause instanceof ExtractorException)
                error = (ExtractorException) cause;
            else
                error = new ExtractorException("Unknown exception", cause);
            throw error;
        }
    }

    public LogEvent getEvent() throws InterruptedException, ExtractorException {
        if (event == null)
            event = fetchEvent();

        return event;
    }
}
