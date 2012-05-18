/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.manage;

import java.util.regex.Pattern;

import com.taobao.common.tedis.replicator.statemachine.Action;
import com.taobao.common.tedis.replicator.statemachine.Event;

/**
 * Defines an event containing an extended command which a regexp specifying
 * states in which the command may be legally processed.
 */
public class ExtendedActionEvent extends Event {
    private final String stateRegexp;
    private final Pattern statePattern;
    private final Action extendedAction;

    public ExtendedActionEvent(String stateRegexp, Action extendedAction) {
        super(null);
        this.stateRegexp = stateRegexp;
        this.extendedAction = extendedAction;
        this.statePattern = Pattern.compile(stateRegexp);
    }

    public String getStateRegexp() {
        return stateRegexp;
    }

    public Action getExtendedAction() {
        return extendedAction;
    }

    public Pattern getStatePattern() {
        return statePattern;
    }
}