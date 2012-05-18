/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine;

public final class TransitionRollbackException extends FiniteStateException {
    private static final long serialVersionUID = 1L;
    private final Event event;
    private final Entity entity;
    private final Transition transition;
    private final int actionType;

    public TransitionRollbackException(String message, Event event, Entity entity, Transition transition, int actionType, Throwable t) {
        super(message, t);
        this.event = event;
        this.entity = entity;
        this.transition = transition;
        this.actionType = actionType;
    }

    public Event getEvent() {
        return event;
    }

    public Entity getEntity() {
        return entity;
    }

    public Transition getTransition() {
        return transition;
    }

    public int getActionType() {
        return actionType;
    }
}