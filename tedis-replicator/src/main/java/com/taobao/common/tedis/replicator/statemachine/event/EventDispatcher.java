/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine.event;

import com.taobao.common.tedis.replicator.statemachine.Event;

public interface EventDispatcher {
    /**
     * Set a listenener for event completion.
     */
    public void setListener(EventCompletionListener listener);

    /**
     * Puts an event in the queue for normal processing. This method returns a
     * Future that callers can call to obtain the event status.
     */
    public EventRequest put(Event event) throws InterruptedException;

    /**
     * Cancel all pending events and put a new event in the queue for immediate
     * processing.
     */
    public EventRequest putOutOfBand(Event event) throws InterruptedException;

    /**
     * Cancel a currently running request.
     *
     * @param request
     *            Request to cancel
     * @param mayInterruptIfRunning
     *            If true we can cancel running as opposed to enqueued request
     */
    public boolean cancelActive(EventRequest request, boolean mayInterruptIfRunning) throws InterruptedException;
}