/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine.event;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.statemachine.Event;
import com.taobao.common.tedis.replicator.statemachine.StateMachine;

public class EventProcessor implements Runnable {
    private static Logger logger = Logger.getLogger(EventProcessor.class);
    private final StateMachine sm;
    private final EventRequest request;
    private final EventCompletionListener listener;

    EventProcessor(StateMachine stateMachine, EventRequest request, EventCompletionListener listener) {
        this.sm = stateMachine;
        this.request = request;
        this.listener = listener;
    }

    public void run() {
        Event event = request.getEvent();
        if (logger.isDebugEnabled())
            logger.debug("Processing event: " + event.getClass().getSimpleName());

        EventStatus status = null;
        try {
            if (request.isCancelRequested()) {
                // If the event is cancelled note it.
                status = new EventStatus(false, true, null);
                if (logger.isDebugEnabled())
                    logger.debug("Skipped cancelled event: " + event.getClass().getSimpleName());
            } else {
                // Mark the request as started and submit to state machine.
                request.started();
                sm.applyEvent(event);
                status = new EventStatus(true, false, null);
                if (logger.isDebugEnabled())
                    logger.debug("Applied event: " + event.getClass().getSimpleName());
            }
        } catch (InterruptedException e) {
            // Handle an interruption, which could happen if we are cancelled
            // while executing.
            status = new EventStatus(false, true, e);
            logger.debug(String.format("Failed to apply event %s, reason=%s", event, e.getLocalizedMessage()));
        } catch (Throwable e) {
            // Handle a failure.
            status = new EventStatus(false, false, e);
            logger.debug(String.format("Failed to apply event %s, reason=%s", event, e.getLocalizedMessage()));
        } finally {
            // We need to store the status and call the completion
            // listener, if any. This must happen regardless of any
            // exception that occurs.
            try {
                if (listener != null)
                    request.setAnnotation(listener.onCompletion(event, status));
            } catch (InterruptedException e) {
                // Do nothing; this is the end of the road for this task.
            } catch (Throwable e) {
                logger.error("Unexpected failure while calling listener", e);
            } finally {
                // Make sure we record the request state no matter what to
                // prevent hangs.
                request.setStatus(status);
            }
        }
    }
}
