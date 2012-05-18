/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine.event;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.taobao.common.tedis.replicator.statemachine.Event;

/**
 * Defines an event request, which contains the event to be processed as well as
 * the status of it. This class implements the Future interface so that it can
 * be returned to clients that track status of events.
 */
public class EventRequest implements Future<EventStatus> {
    private final EventDispatcher dispatcher;
    private final Event event;
    private boolean cancelRequested = false;
    private boolean started = false;
    private EventStatus status;
    private Object annotation;

    EventRequest(EventDispatcher dispatcher, Event event) {
        this.dispatcher = dispatcher;
        this.event = event;
    }

    public synchronized Event getEvent() {
        return event;
    }

    public synchronized void setAnnotation(Object annotation) {
        this.annotation = annotation;
    }

    public synchronized Object getAnnotation() {
        return annotation;
    }

    public synchronized void started() {
        started = true;
    }

    /**
     * Sets the status on a processed event and notifies anyone waiting for
     * status to arrive.
     */
    public synchronized void setStatus(EventStatus status) {
        this.status = status;
        this.notifyAll();
    }

    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
        // Perform cancellation based on where we are.
        if (!started) {
            // If we have not started, just mark ourselves for cancellation.
            this.cancelRequested = true;
            return true;
        } else if (isDone()) {
            // Cannot cancel after we are finished.
            return false;
        } else {
            // We are not done and not running, so try to cancel.
            try {
                return dispatcher.cancelActive(this, mayInterruptIfRunning);
            } catch (InterruptedException e) {
                // Show that we were interrupted. This seems kind of unlikely.
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    public synchronized EventStatus get() throws InterruptedException, ExecutionException {
        while (!isDone()) {
            wait();
        }
        return status;
    }

    public synchronized EventStatus get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        // Try to wait on status.
        if (!isDone()) {
            // Normalize time units to millis. To prevent nanoseconds and
            // microseconds from being cleared to 0, make sure that any
            // non-zero value results in 1ms. Java changed its mind
            // about time units in a kind of messy way, hence this code.
            long timeoutMillis = convertTimeToMillis(timeout, unit);
            if (timeout > 0 && timeoutMillis == 0)
                timeoutMillis = 1;
            this.wait(timeoutMillis);
        }

        // If we finished, return status; otherwise signal a timeout.
        if (isDone())
            return status;
        else
            throw new TimeoutException();
    }

    public synchronized boolean isCancelRequested() {
        return cancelRequested;
    }

    public synchronized boolean isCancelled() {
        return (status != null && status.isCancelled());
    }

    public synchronized boolean isDone() {
        return (status != null);
    }

    // Converts time to milliseconds.
    public long convertTimeToMillis(long time, TimeUnit unit) {
        switch (unit) {
        case NANOSECONDS:
            return time / (1000 * 1000);
        case MICROSECONDS:
            return time / 1000;
        case MILLISECONDS:
            return time;
        case SECONDS:
            return time * 1000;
        case MINUTES:
            return time * 1000 * 60;
        case HOURS:
            return time * 1000 * 60 * 60;
        case DAYS:
            return time * 1000 * 60 * 60 * 24;
        default:
            return time;
        }
    }
}