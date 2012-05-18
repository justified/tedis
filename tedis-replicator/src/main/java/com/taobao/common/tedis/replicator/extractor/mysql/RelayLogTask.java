/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import org.apache.log4j.Logger;

public class RelayLogTask implements Runnable {
    private static Logger logger = Logger.getLogger(RelayLogTask.class);
    private RelayLogClient relayClient;
    private volatile boolean cancelled = false;
    private volatile boolean finished = false;

    public RelayLogTask(RelayLogClient relayClient) {
        this.relayClient = relayClient;
    }

    public void run() {
        logger.info("Relay log task starting: " + Thread.currentThread().getName());
        try {
            while (!cancelled && !Thread.currentThread().isInterrupted()) {
                relayClient.processEvent();
            }
        } catch (InterruptedException e) {
            logger.info("Relay log task cancelled by interrupt");
        } catch (Throwable t) {
            logger.error("Relay log task failed due to exception: " + t.getMessage(), t);
        } finally {
            relayClient.disconnect();
        }

        logger.info("Relay log task ending: " + Thread.currentThread().getName());
        finished = true;
    }

    public void cancel() {
        cancelled = true;
    }

    public boolean isFinished() {
        return finished;
    }

    public RelayLogPosition getPosition() {
        return relayClient.getPosition();
    }
}