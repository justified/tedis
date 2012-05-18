/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine;

import java.util.List;
import java.util.Vector;

public class TransitionMatcher {
    Vector<Transition> transitions = new Vector<Transition>();

    public TransitionMatcher() {
    }

    public void addTransition(Transition transition) {
        transitions.add(transition);
    }

    public List<Transition> getTransitions() {
        return transitions;
    }

    public Transition matchTransition(Event event, Entity entity) {
        for (Transition transition : transitions) {
            if (transition.accept(event, entity)) {
                return transition;
            }
        }
        return null;
    }
}