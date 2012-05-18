/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.pipeline;

import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplEvent;

public interface Schedule {
    public int advise(ReplEvent replEvent) throws InterruptedException;

    public void taskEnd();

    public void setLastProcessedEvent(ReplDBMSEvent event) throws InterruptedException;

    public boolean isCancelled();

    public static int PROCEED = 1;

    public static int QUIT = 2;

    public static int CONTINUE_NEXT = 3;

    public static int CONTINUE_NEXT_COMMIT = 4;
}