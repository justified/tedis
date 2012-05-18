/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.dislock;


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.taobao.common.tedis.util.StringUtil;
import com.taobao.common.tedis.util.ZKUtil;

public class ZKLock implements Lock, ReconnectWare {

    public static Log logger = LogFactory.getLog(ZKLock.class);

    private String appId;
    private String taskName;
    private ZKClient client;
    private List<Register> registers;

    public ZKLock(String appId, String taskName, ZKClient client) {
        this.appId = appId;
        this.taskName = taskName;
        this.client = client;
        if (client != null) client.addWare(this);
    }

    public boolean isOwner() {
        if (client == null || !client.useAble()) return false;
        String path = genLockPath(appId, taskName);
        String ip = StringUtil.getLocalHostIP();
        try {
            if (client.exists(path)) {
                String owner = new String(client.getData(path));
                if (ip.equals(owner)) return true;
            }
        } catch (Exception e) {
            logger.debug("- ignore this exception " + e);
        }
        return false;
    }

    public boolean tryLock() {
        if (client == null || !client.useAble()) return false;
        String path = genLockPath(appId, taskName);
        String ip = StringUtil.getLocalHostIP();
        try {
            if (client.exists(path)) {
                String owner = null;
                try {
                    owner = new String(client.getData(path));
                } catch (Exception e) {
                    return tryLock();
                }
                logger.warn("- path:" + path + " exists data " + owner + " - " + ip);
                if (ip.equals(owner)) return true;
            } else {
                logger.warn("- path:" + path + " not exists data");
                client.createPathIfAbsent(path, ZKUtil.toBytes(ip), false);
                if (client.exists(path)) {
                    String owner = new String(client.getData(path));
                    logger.warn("- path:" + path + " create data " + owner + " - " + ip);
                    if (ip.equals(owner)) return true;
                }
            }
        } catch (Exception e) {
            logger.warn("- ignore this exception " + e);
        }
        return false;
    }

    public boolean lock() {
        return false;
    }

    public boolean unlock() {
        if (client == null || !client.useAble()) return false;
        String path = genLockPath(appId, taskName);
        String ip = StringUtil.getLocalHostIP();
        try {
            if (client.exists(path)) {
                String owner = new String(client.getData(path));
                if (ip.equals(owner)) {
                    client.delete(path);
                }
            }
        } catch (Exception e) {
            logger.debug("- ignore this exception " + e);
        }
        return true;
    }

    public void registerListener(CallBack call, Object ctx) {
        registerListenerIn(call, ctx, true);
    }

    private void registerListenerIn(CallBack call, Object ctx, boolean out) {
        if (client == null || !client.useAble() || call == null) throw new RuntimeException("");
        if (out) {
            Register register = new Register(call, ctx);
            if (registers == null) registers = new ArrayList<Register>();
            registers.add(register);
        }
        String path = genLockPath(appId, taskName);
        try {
            client.exists(path, new NodeListener(path, call, ctx));
        } catch (Exception e) {
            logger.debug("- ignore this exception " + e);
        }
    }

    private class Register {

        private CallBack call;
        private Object ctx;
        public Register(CallBack call, Object ctx) {
            this.call = call;
            this.ctx = ctx;
        }
        public CallBack getCall() {
            return call;
        }
        public Object getCtx() {
            return ctx;
        }
    }

    public class NodeListener implements Watcher {

        private CallBack back;
        private Object ctx;
        private String path;

        public NodeListener(String path, CallBack back, Object ctx) {
            this.path = path;
            this.back = back;
            this.ctx = ctx;
        }

        public void checkLock() {
            try {
                back.doTask(path, ctx);
            } catch (Exception e) {
                logger.debug("- catch exception during callback " + e);
            }
        }

        public void process(WatchedEvent event) {
            EventType type = event.getType();
            if (type == EventType.None) {
                if (event.getState() == KeeperState.SyncConnected) checkLock();
                else return;
            } else if (type == EventType.NodeDeleted) {
                checkLock();
            }
            try {
                client.exists(path, NodeListener.this);
            } catch (Exception e) {
                logger.debug("- catch exception during register listener ...");
            }
        }
    }

    public static String genLockPath(String appId, String taskName) {
        String path = ZKUtil.contact(appId, taskName);
        path = ZKUtil.contact(ZKUtil.MUTEXLOCK_ROOT, path);
        path = ZKUtil.contact(path, ZKUtil.LOCK_OWNER);
        path = ZKUtil.normalize(path);
        return path;
    }

    public static String genAppPath(String appId) {
        String path = ZKUtil.contact(ZKUtil.MUTEXLOCK_ROOT, appId);
        path = ZKUtil.normalize(path);
        return path;
    }

    public void reconnect() {
        if (registers != null) {
            for (Register register : registers) {
                registerListenerIn(register.getCall(), register.getCtx(), false);
            }
        }
    }

}
