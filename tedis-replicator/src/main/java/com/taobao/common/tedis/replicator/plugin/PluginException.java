/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.plugin;

import com.taobao.common.tedis.replicator.ReplicatorException;

public class PluginException extends ReplicatorException {

    private static final long serialVersionUID = 1L;

    /**
     *
     * Creates a new <code>PluginException</code> object
     *
     * @param msg
     */
    public PluginException(String msg) {
        super(msg);
    }

    /**
     *
     * Creates a new <code>PluginException</code> object
     *
     * @param throwable
     */
    public PluginException(Throwable throwable) {
        super(throwable);
    }

    /**
     *
     * Creates a new <code>PluginException</code> object
     *
     * @param msg
     * @param throwable
     */
    public PluginException(String msg, Throwable throwable) {
        super(msg, throwable);
    }

}
