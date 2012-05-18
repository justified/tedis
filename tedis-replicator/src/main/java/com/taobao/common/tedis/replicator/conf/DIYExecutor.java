/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.conf;

import java.util.concurrent.Executor;

public class DIYExecutor implements Executor {
    private static final DIYExecutor instance = new DIYExecutor();
    public static DIYExecutor getInstance() {
        return instance;
    }

    public void execute(Runnable task) {
        task.run();
    }

}