/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.dislock;

/**
 * most of the lock implements are object waiting blocking
 * so pay attention to thread.interrupt mark
 */
public interface Lock {

    /**
     * try to get the lock, return true if success or false immediately
     * return true if this machine already hold the lock
     * return false if ZooKeeper service is disable
     */
    public boolean tryLock();

    /**
     * wait until currently runnable get the lock or interrupted
     * return false if ZooKeeper service is disable
     */
    public boolean lock();

    /**
     * release the lock and return true if success
     * return false if ZooKeeper service is disable
     */
    public boolean unlock();

    /**
     * return true if currently runnable get the lock
     * return false if ZooKeeper service is disable
     */
    public boolean isOwner();

    /**
     * register listener to prescribed lock
     * when currently runnable get the lock will call call.doTask
     * and ctx is the context
     * throw a RunTimeException if ZooKeeper service is disable
     */
    public void registerListener(CallBack call, Object ctx);

}

