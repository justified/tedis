/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.manage.events;

import com.taobao.common.tedis.replicator.ReplicatorProperties;
import com.taobao.common.tedis.replicator.statemachine.Event;

/**
 * Signals that the replicator should move to the off-line state. This event may
 * be submitted by underlying code to initiate a controlled shutdown.
 */
public class GoOfflineEvent extends Event {
    private ReplicatorProperties params;

    public GoOfflineEvent() {
        this(new ReplicatorProperties());
    }

    public GoOfflineEvent(ReplicatorProperties params) {
        super(null);
        this.params = params;
    }

    public ReplicatorProperties getParams() {
        return params;
    }
}
