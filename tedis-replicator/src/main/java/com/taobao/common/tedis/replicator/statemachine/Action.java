/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine;

public interface Action {
    public static final int EXIT_ACTION = 1;

    public static final int TRANSITION_ACTION = 2;

    public static final int ENTER_ACTION = 3;

    public void doAction(Event message, Entity entity, Transition transition, int actionType) throws TransitionRollbackException, TransitionFailureException, InterruptedException;
}