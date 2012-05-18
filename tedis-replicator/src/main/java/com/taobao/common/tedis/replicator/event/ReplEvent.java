/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.event;

import java.io.Serializable;

public abstract class ReplEvent implements Serializable {
    private static final long serialVersionUID = 1300;
    private transient int estimatedSize;

    public ReplEvent() {
    }

    public abstract long getSeqno();

    public int getEstimatedSize() {
        return estimatedSize;
    }

    public void setEstimatedSize(int estimatedSize) {
        this.estimatedSize = estimatedSize;
    }
}
