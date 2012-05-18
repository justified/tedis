/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.filter;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.plugin.PluginContext;

public class LoggingFilter implements Filter {
    private static Logger logger = Logger.getLogger(LoggingFilter.class);

    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException {
        try {
            if (event != null) {
                StringBuilder stringBuilder = new StringBuilder();
                logger.info("Filtered event: " + stringBuilder.toString().trim());
            } else
                logger.info("Filtered event: null");
        } catch (Exception e) {
            logger.warn("Exception on logging event: " + e.getStackTrace());
        }
        return event;
    }

    public void configure(PluginContext context) throws ReplicatorException {
    }

    public void prepare(PluginContext context) throws ReplicatorException {
    }

    public void release(PluginContext context) throws ReplicatorException {
    }
}
