/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.util;

import java.util.List;
import java.util.Vector;

public class WatchManager<E> {
    private List<Watch<E>> watchList = new Vector<Watch<E>>();
    boolean cancelled = false;

    public WatchManager() {
    }

    public synchronized Watch<E> watch(WatchPredicate<E> predicate, int taskCount, WatchAction<E> action) {
        assertNotCancelled();
        Watch<E> watch = new Watch<E>(predicate, taskCount, action);
        watchList.add(watch);
        return watch;
    }

    public synchronized Watch<E> watch(WatchPredicate<E> predicate, int taskCount) {
        return watch(predicate, taskCount, null);
    }

    public synchronized void process(E event, int taskId) throws InterruptedException {
        assertNotCancelled();
        // Walk backwards down list to avoid ConcurrentModificationException
        // from using an Iterator. Note we also clean out anything that is
        // done; this is how cancelled watches are removed.
        for (int i = watchList.size() - 1; i >= 0; i--) {
            Watch<E> watch = watchList.get(i);
            if (watch.isDone())
                watchList.remove(watch);
            else if (watch.offer(event, taskId)) {
                // Execute the watch action.
                WatchAction<E> action = watch.getAction();
                if (action != null) {
                    action.matched(event, taskId);
                }

                // Dequeue if watch is fulfilled.
                if (watch.isDone())
                    watchList.remove(watch);
            }
        }
    }

    public synchronized void cancelAll() {
        assertNotCancelled();
        for (Watch<E> w : watchList) {
            w.cancel(true);
        }
        cancelled = true;
    }

    private void assertNotCancelled() {
        if (cancelled)
            throw new IllegalStateException("Operation submitted after cancellation");
    }
}