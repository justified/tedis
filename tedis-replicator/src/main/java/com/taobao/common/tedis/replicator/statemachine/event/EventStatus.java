/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine.event;

public class EventStatus {
    private final boolean successful;
    private final boolean cancelled;
    private final Throwable exception;

    public EventStatus(boolean successful, boolean cancelled, Throwable exception) {
        this.successful = successful;
        this.cancelled = cancelled;
        this.exception = exception;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public Throwable getException() {
        return exception;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public boolean equals(Object o) {
        if (!(o instanceof EventStatus))
            return false;
        EventStatus other = (EventStatus) o;
        if (successful != other.isSuccessful())
            return false;
        else if (cancelled != other.isCancelled())
            return false;
        else if (exception != other.getException())
            return false;
        else
            return true;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getName());
        sb.append(" successful=").append(successful);
        sb.append(" cancelled=").append(cancelled);
        sb.append(" exception=").append(exception);
        return sb.toString();
    }
}
