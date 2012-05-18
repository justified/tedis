/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.conf;

public class ServerRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ServerRuntimeException(String msg) {
        super(msg);
    }

    public ServerRuntimeException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
