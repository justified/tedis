/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine;

public class Transition {
    private final String name;
    private final Guard guard;
    private final State input;
    private final Action action;
    private final State output;

    public Transition(String name, Guard guard, State input, Action action, State output) {
        this.name = name;
        this.guard = guard;
        this.input = input;
        this.action = action;
        this.output = output;
    }

    public Transition(Guard guard, State input, Action action, State output) {
        this(String.format("%s_TO_%s", input.getName(), output.getName()), guard, input, action, output);
    }

    public String getName() {
        return name;
    }

    public Guard getGuard() {
        return guard;
    }

    public State getInput() {
        return input;
    }

    public Action getAction() {
        return action;
    }

    public State getOutput() {
        return output;
    }

    public boolean accept(Event event, Entity entity) {
        return guard.accept(event, entity, input);
    }

    public String toString() {
        return name;
    }
}