/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class State {
    private final String name;
    private final StateType type;
    private final State parent;
    private final Action entryAction;
    private final Action exitAction;

    private final String qualifiedName;
    private final State[] hierarchy;
    private List<State> children = new ArrayList<State>();

    public State(String name, StateType type, State parent, Action entryAction, Action exitAction) {
        this.name = name;
        this.type = type;
        this.parent = parent;
        this.entryAction = entryAction;
        this.exitAction = exitAction;

        // Compute name and enclosing state hierarchy.
        if (parent == null) {
            this.qualifiedName = name;
            this.hierarchy = new State[] { this };
        } else {
            this.qualifiedName = parent.getName() + ":" + name;
            State[] parentArray = parent.getHierarchy();
            State[] selfArray = new State[parentArray.length + 1];
            for (int i = 0; i < parentArray.length; i++)
                selfArray[i] = parentArray[i];
            selfArray[selfArray.length - 1] = this;
            this.hierarchy = selfArray;
        }

        if (parent == null) {

        }
    }

    public State(Enum<?> stateEnum, StateType type, State parent, Action entryAction, Action exitAction) {
        this(stateEnum.toString(), type, parent, entryAction, exitAction);
    }

    public State(String name, StateType type, Action entryAction, Action exitAction) {
        this(name, type, null, entryAction, exitAction);
    }

    public State(String name, StateType type) {
        this(name, type, null, null, null);
    }

    public State(Enum<?> stateEnum, StateType type) {
        this(stateEnum, type, null, null, null);
    }

    public State(String name, StateType type, State parent) {
        this(name, type, parent, null, null);
    }

    void addChild(State state) {
        children.add(state);
    }

    public List<State> getChildren() {
        return Collections.unmodifiableList(children);
    }

    public Action getEntryAction() {
        return entryAction;
    }

    public Action getExitAction() {
        return exitAction;
    }

    public String getName() {
        return qualifiedName;
    }

    public String getBaseName() {
        return name;
    }

    public StateType getType() {
        return type;
    }

    public boolean isStart() {
        return type == StateType.START;
    }

    public boolean isEnd() {
        return type == StateType.END;
    }

    public State getParent() {
        return parent;
    }

    public State[] getHierarchy() {
        return hierarchy;
    }

    public boolean isSubstate() {
        return parent != null;
    }

    public boolean isSubstateOf(State other) {
        if (parent == null)
            return false;
        else if (parent == other)
            return true;
        else
            return parent.isSubstateOf(other);
    }

    public State getLeastCommonParent(State other) {
        State least = null;
        State[] otherHierarchy = other.getHierarchy();
        for (int i = 0; i < hierarchy.length; i++) {

            if (otherHierarchy.length <= i)
                break;
            else if (hierarchy[i] == other.getHierarchy()[i])
                least = hierarchy[i];
            else
                break;
        }
        return least;
    }

    public String toString() {
        return getName();
    }

    public boolean equals(Object o) {
        if (o != null && o instanceof State) {
            String otherName = ((State) o).getName();
            if (qualifiedName == null)
                return qualifiedName == otherName;
            else
                return qualifiedName.equals(otherName);
        } else
            return false;
    }
}