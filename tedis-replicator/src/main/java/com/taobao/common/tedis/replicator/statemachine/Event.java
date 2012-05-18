/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine;

public class Event {
    private final Object data;

    public Event(Object data) {
        this.data = data;
    }

    public Object getData() {
        return data;
    }

    public String toString() {
        String className = getClass().getSimpleName();
        int internalClassSign = 0;
        if ((internalClassSign = className.indexOf("$")) != -1) {
            return "Event:" + className.substring(internalClassSign + 1);
        }
        return "Event:" + className;
    }
}