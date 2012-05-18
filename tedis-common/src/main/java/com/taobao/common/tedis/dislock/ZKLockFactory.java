/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.dislock;


import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.tedis.util.StringUtil;
import com.taobao.common.tedis.util.ZKUtil;

public class ZKLockFactory extends LockFactory {

    protected static final String PRO_ZKADDRESS = "zkAddress";
    protected static final String PRO_ZKTIMEOUT = "zkTimeout";
    protected static final String PRO_APPNAME   = "appName";
    public static Log logger = LogFactory.getLog(ZKLockFactory.class);

    private String zkAddress = null;
    private String zkTimeout = null;
    private String appName = null;
    private ZKClient client = null;
    private boolean inited = false;

    public Lock getLock(String name) {
        if (inited && createNode(name)) {
            ZKLock lock = new ZKLock(appName, name, client);
            return lock;
        } else {
            logger.debug("- ZKLockFactory hasn't been init or create task path fail ...");
            return null;
        }
    }

    public void init() throws Exception {
        String exception = "";
        if(properties != null){
            zkAddress = properties.getProperty(PRO_ZKADDRESS);
            zkTimeout = properties.getProperty(PRO_ZKTIMEOUT);
            appName   = properties.getProperty(PRO_APPNAME);
            if (StringUtil.isBlank(zkAddress) || StringUtil.isBlank(zkTimeout) ||
            StringUtil.isBlank(appName)) {
                exception = "LockFactory's porperties hasn't been right set ...";
            } else {
                int timeout = Integer.parseInt(zkTimeout);
                if (timeout < 3000) {
                    exception = "LockFactory's porperty zkTimeout mustn't less then 3000 ...";
                } else {
                    client = new ZKClient(zkAddress, timeout);
                    try {
                        client.init();
                    } catch (Exception e) {
                        logger.error("error found during init zkLock " + e);
                        throw e;
                    }
                    try {
                        List<String> children = client.getChildren(ZKLock.genAppPath(appName));
                        for (String str : children) {
                            if (StringUtil.isBlank(str)) continue;
                            String lockPath = ZKLock.genLockPath(appName, str);
                            if (client.exists(lockPath)) {
                                String owner = null;
                                try {
                                    owner = new String(client.getData(lockPath));
                                } catch (Exception e) {
                                    continue;
                                }
                                if (StringUtil.isLastConn(owner)) {
                                    try {
                                        client.delete(lockPath);
                                    } catch (Exception e) {
                                    }
                                    break;
                                }
                            }
                        }
                    } catch (Exception e) {
                    }
                    inited = true;
                    return;
                }
            }
        } else {
            exception = "LockFactory's porperties hasn't been set ...";
        }
        logger.error("error found during init zkLock " + exception);
        throw new Exception(exception);
    }

    private boolean createNode(String taskName) {
        if (client == null || !client.useAble() || StringUtil.isBlank(taskName)) return false;
        String path = ZKUtil.contact(appName, taskName);
        path = ZKUtil.contact(ZKUtil.MUTEXLOCK_ROOT, path);
        path = ZKUtil.normalize(path);
        try {
            if (!client.exists(path)) client.rcreate(path, null);
            return true;
        } catch (Exception e) {
        }
        return false;
    }
}
