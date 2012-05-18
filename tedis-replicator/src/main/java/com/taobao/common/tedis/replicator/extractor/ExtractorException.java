/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor;

import com.taobao.common.tedis.replicator.ReplicatorException;

public class ExtractorException extends ReplicatorException {
    static final long serialVersionUID = 1L;
    private final String eventId;

    public ExtractorException(String msg) {
        this(msg, null, null);
    }

    public ExtractorException(Throwable t) {
        this(null, t, null);
    }

    public ExtractorException(String msg, Throwable cause) {
        this(msg, cause, null);
    }

    public ExtractorException(String msg, Throwable cause, String eventId) {
        super(msg, cause);
        this.eventId = eventId;
    }

    public String getEventId() {
        return eventId;
    }
}
