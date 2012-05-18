/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.manage;

import com.taobao.common.tedis.replicator.ReplicatorException;

public class ReplicatorStateException extends ReplicatorException {
    private static final long serialVersionUID = 1L;

    public ReplicatorStateException(String message) {
        super(message);
    }

    public ReplicatorStateException(String message, Throwable e) {
        super(message, e);
    }
}