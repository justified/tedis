/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine;

public class TransitionNotFoundException extends FiniteStateException {
    private static final long serialVersionUID = 1L;

    private final State state;
    private final Event event;
    private final Entity entity;

    public TransitionNotFoundException(String message, State state, Event event, Entity entity) {
        super(message);
        this.state = state;
        this.event = event;
        this.entity = entity;
    }

    public State getState() {
        return state;
    }

    public Event getEvent() {
        return event;
    }

    public Entity getEntity() {
        return entity;
    }
}