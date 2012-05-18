/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.pipeline;

import com.taobao.common.tedis.replicator.event.ReplControlEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.event.ReplEvent;

/**
 * Defines a basic schedule implementation that tracks watches on events and
 * task termination logic.
 */
public class SimpleSchedule implements Schedule {
    private final Stage stage;
    private final SingleThreadStageTask task;

    public SimpleSchedule(Stage stage, SingleThreadStageTask task) {
        this.stage = stage;
        this.task = task;
    }

    public int advise(ReplEvent replEvent) throws InterruptedException {
        // Fix up cancellation logic.
        if (replEvent instanceof ReplDBMSEvent) {
            ReplDBMSEvent event = (ReplDBMSEvent) replEvent;
            if (stage.getProgressTracker().skip(event))
                return CONTINUE_NEXT_COMMIT;
            else
                return PROCEED;
        } else if (replEvent instanceof ReplControlEvent) {
            ReplControlEvent controlEvent = (ReplControlEvent) replEvent;
            if (controlEvent.getEventType() == ReplControlEvent.STOP)
                return QUIT;
            else if (controlEvent.getEventType() == ReplControlEvent.SYNC) {
                ReplDBMSHeader syncEvent = controlEvent.getHeader();
                stage.getProgressTracker().setLastProcessedEvent(task.getTaskId(), syncEvent);
                return CONTINUE_NEXT;
            } else
                throw new RuntimeException("Unsupported control type: " + controlEvent.getEventType());
        } else
            throw new RuntimeException("Unsupported event type: " + replEvent.getClass().toString());
    }

    public synchronized boolean isCancelled() {
        return stage.getProgressTracker().isCancelled(task.getTaskId());
    }

    public synchronized void setLastProcessedEvent(ReplDBMSEvent event) throws InterruptedException {
        stage.getProgressTracker().setLastProcessedEvent(task.getTaskId(), event);
    }

    public synchronized void taskEnd() {
        stage.getTaskGroup().reportTaskShutdown(Thread.currentThread(), task);
    }

    public synchronized boolean skip(ReplDBMSEvent event) throws InterruptedException {
        return stage.getProgressTracker().skip(event);
    }

    public synchronized void cancel() {
        stage.getProgressTracker().cancel(task.getTaskId());
    }
}
