/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

public class StateTransitionLatch implements Callable<State>, StateChangeListener {
    private final StateMachine stateMachine;
    private final State expected;
    private final boolean endOnError;

    private State errorState;
    private State current;
    private boolean done;
    private boolean reachedExpected;
    private boolean reachedError;
    private BlockingQueue<State> stateQueue = new LinkedBlockingQueue<State>();

    StateTransitionLatch(StateMachine sm, State expected, boolean endOnError) {
        this.stateMachine = sm;
        this.expected = expected;
        this.endOnError = endOnError;
        errorState = sm.getErrorState();
        // NOTE: The following lines require synchronization on the state
        // machine.
        stateMachine.addListener(this);
        this.stateQueue.add(stateMachine.getState());
    }

    public boolean isDone() {
        return done;
    }

    public boolean isExpected() {
        return reachedExpected;
    }

    public boolean isError() {
        return reachedError;
    }

    protected synchronized State getCurrent() {
        return current;
    }

    protected synchronized void setCurrent(State state) {
        this.current = state;
    }

    public synchronized void stateChanged(Entity entity, State oldState, State newState) {
        stateQueue.add(newState);
    }

    public State call() {
        try {
            // Run until we finish or somebody interrupts us.
            while (!done && !Thread.interrupted()) {
                try {
                    setCurrent(stateQueue.take());
                    // Use startsWith to handle parent states.
                    if (current.getName().startsWith(expected.getName())) {
                        done = true;
                        reachedExpected = true;
                    } else if (endOnError && errorState != null && current.equals(errorState)) {
                        done = true;
                        reachedError = true;
                    }
                } catch (InterruptedException e) {
                    // Interruption means somebody is trying to cancel us.
                    break;
                }
            }
        } finally {
            stateMachine.removeListener(this);
        }

        // If we are done, return the current state. Otherwise, return null.
        if (done)
            return current;
        else
            return null;
    }
}