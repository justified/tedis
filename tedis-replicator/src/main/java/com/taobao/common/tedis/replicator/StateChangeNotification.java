/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator;

import java.io.Serializable;

public class StateChangeNotification implements Serializable {

    static final long serialVersionUID = 1L;
    String prevState = null;
    String newState = null;
    String cause = null;

    public StateChangeNotification(String prevState, String newState, String cause) {
        this.prevState = prevState;
        this.newState = newState;
        this.cause = cause;
    }

    public String getPrevState() {
        return prevState;
    }

    public String getNewState() {
        return newState;
    }

    public String getCause() {
        return cause;
    }

}
