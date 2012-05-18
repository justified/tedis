/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine.event;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.statemachine.Event;
import com.taobao.common.tedis.replicator.statemachine.StateMachine;

/**
 * This class defines an event dispatcher task, which is a separate thread that
 * dispatches events to a listener from a queue. It handles normal events,
 * out-of-band events, and event cancellation.
 */
public class EventDispatcherTask implements Runnable, EventDispatcher {
    private static Logger logger = Logger.getLogger(EventDispatcherTask.class);

    // Variables to define the state machine.
    private StateMachine stateMachine = null;
    private Thread dispatcherThread = null;
    private boolean cancelled = false;
    private EventRequest currentRequest = null;
    private Future<?> submittedEvent = null;
    private EventCompletionListener listener;
    private BlockingQueue<EventRequest> notifications = new LinkedBlockingQueue<EventRequest>();

    public EventDispatcherTask(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    public void setListener(EventCompletionListener listener) {
        this.listener = listener;
    }

    public boolean isRunning() {
        return (dispatcherThread != null);
    }

    public void run() {
        // Allocate a thread pool to process each succeeding event.
        ExecutorService pool = Executors.newFixedThreadPool(1);
        try {
            while (!cancelled) {
                // Prepare to submit next event. We synchronize on the
                // request queue to avoid race conditions with cancellation.
                synchronized (notifications) {
                    // These operations must be serialized with cancellation.
                    while (notifications.isEmpty())
                        notifications.wait();
                    currentRequest = notifications.take();
                    EventProcessor eventProcessor = new EventProcessor(stateMachine, currentRequest, listener);
                    submittedEvent = pool.submit(eventProcessor);
                }

                // Wait for the event to complete or be cancelled.
                try {
                    // Ensure the request is completed.
                    currentRequest.get();
                } catch (CancellationException e) {
                    if (logger.isDebugEnabled())
                        logger.debug(String.format("Event processing cancelled=%s", currentRequest.getEvent()));
                    if (!currentRequest.isCancelled()) {
                        // Set status to cover possible race conditions if
                        // request thread is cancelled before it can set status.
                        currentRequest.setStatus(new EventStatus(false, true, e));
                    }
                }

                // Show that we have completed processing.
                synchronized (notifications) {
                    // Synchronized to avoid race conditions with cancellation.
                    currentRequest = null;
                    submittedEvent = null;
                }
            }
        } catch (InterruptedException e) {
            logger.debug("Dispatcher loop terminated by InterruptedException");
        } catch (Throwable t) {
            logger.error("Dispatcher loop terminated by unexpected exception", t);
        }
        logger.info("Dispatcher thread terminating");
    }

    public EventRequest put(Event event) throws InterruptedException {
        if (event instanceof OutOfBandEvent)
            return putOutOfBand(event);
        else
            return putInternal(event);
    }

    public EventRequest putOutOfBand(Event event) throws InterruptedException {
        synchronized (notifications) {
            cancelAll();
            return putInternal(event);
        }
    }

    private EventRequest putInternal(Event event) throws InterruptedException {
        synchronized (notifications) {
            EventRequest request = new EventRequest(this, event);
            notifications.put(request);
            notifications.notifyAll();
            return request;
        }
    }

    private void cancelAll() throws InterruptedException {
        synchronized (notifications) {
            // Cancel all pending requests.
            for (EventRequest request : notifications) {
                request.cancel(true);
            }

            // If there is an executing request, cancel that too.
            if (this.submittedEvent != null) {
                submittedEvent.cancel(true);
            }
        }
    }

    public boolean cancelActive(EventRequest request, boolean mayInterruptIfRunning) throws InterruptedException {
        synchronized (notifications) {
            // If our request is executing and we are permitted, cancel it.
            if (currentRequest == request && mayInterruptIfRunning) {
                submittedEvent.cancel(true);
                return true;
            } else
                return false;
        }
    }

    /**
     * Start the event dispatcher, which spawns a separate thread.
     *
     * @param name
     *            Name of the dispatcher thread
     */
    public synchronized void start(String name) throws Exception {
        logger.debug("Starting event dispatcher");
        if (dispatcherThread != null)
            throw new Exception("Dispatcher thread already started");
        if (name == null)
            name = this.getClass().getSimpleName();
        dispatcherThread = new Thread(this, name);
        dispatcherThread.start();
    }

    /**
     * Cancel the event dispatcher and wait for the thread to complete.
     *
     * @param graceful
     *            If true stop nicely after all pending events have cleared
     */
    public synchronized void stop() throws InterruptedException {
        if (dispatcherThread == null)
            return;
        logger.info("Requesting dispatcher thread termination: name=" + dispatcherThread.getName());
        cancelled = true;
        cancelAll();
        dispatcherThread.interrupt();
        dispatcherThread.join();
        dispatcherThread = null;
    }
}