/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.dislock;


import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import com.taobao.common.tedis.util.ZKUtil;

public class ZKClient implements Watcher {

    public int DEFAULTTIME = 3000;
    public static Log logger = LogFactory.getLog(ZKClient.class);

    public String zkAddress;
    public int timeout = DEFAULTTIME;

    private ZooKeeper zookeeper = null;
    private CountDownLatch connectedSignal;
    private ReentrantLock connectLock = new ReentrantLock();

    private HashSet<ReconnectWare> wares = new HashSet<ReconnectWare> ();

    public ZKClient(String address, int timeout) {
        this.zkAddress = address;
        this.timeout = timeout;
    }

    public void init() throws Exception {
        connectedSignal = new CountDownLatch(1);
        zookeeper = new ZooKeeper(zkAddress, timeout, this);
        if (connectedSignal.await(6000, TimeUnit.MILLISECONDS)) {
            logger.warn("- the conect to zookeeper server success ...");
        } else {
            logger.error("- try to establish connection to zookeeper timeout ...");
            throw new Exception("- try to establish connection to zookeeper timeout ...");
        }
    }

    public void addWare(ReconnectWare ware) {
        this.wares.add(ware);
    }

    synchronized public void process(WatchedEvent event) {
        KeeperState state = event.getState();
        if (state == KeeperState.SyncConnected) {
            connectedSignal.countDown();
        } else if (state == KeeperState.Expired) {
            logger.warn("- the connect to ZooKeeper session expired ...");
            connect();
        } else if(state == KeeperState.Disconnected){
            logger.warn("- the zookeeper state is disconnected and it will connect later ...");
        } else {
            logger.warn("- the zookeeper state is " + state);
        }
    }

    public void connect() {
        if (connectLock.tryLock()) {
            for (int i = 0; i < 5; i++) {
                try {
                    zookeeper = new ZooKeeper(zkAddress, timeout, this);
                    while (true) {
                        Thread.sleep(1000);
                        States state = zookeeper.getState();
                        if (state == States.CONNECTED) {
                            notifyWare();
                            return;
                        } else if (state == States.CLOSED || state == States.AUTH_FAILED) {
                            break;
                        }
                    }
                } catch (IOException e) {
                    logger.error("- exception appear during new zookeeper after expired " + e);
                } catch (Exception e) {
                }
            }
            connectLock.unlock();
        }
    }

    private void notifyWare() {
        for (ReconnectWare ware : this.wares) {
            if (ware != null) {
                try {
                    ware.reconnect();
                } catch (Exception e) {
                }
            }
        }
    }

    public boolean useAble() {
        return zookeeper != null && zookeeper.getState() == ZooKeeper.States.CONNECTED;
    }

    public void create(String path, byte[] bytes, boolean isPersistent) throws ZKException {
        path = ZKUtil.normalize(path);
        CreateMode createMode = isPersistent? CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
        try {
            zookeeper.create(path, bytes, Ids.OPEN_ACL_UNSAFE, createMode);
        } catch (KeeperException e) {
            if(e instanceof KeeperException.NoNodeException) {
                throw new ZKException("The node ["  + e.getPath() + "]'s parent node doesn't exist,can't create it. ", e);
            } else if(e instanceof KeeperException.NodeExistsException) {
                this.setData(path, bytes);
            } else {
                throw new ZKException("Other error", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ZKException(e);
        } catch(Throwable e){
            throw new ZKException(e);
        }
    }

    public void create(String path,byte[] bytes) throws ZKException {
        this.create(path, bytes, true);
    }

    public void rcreate(String path,byte[] bytes)throws ZKException {
        this.rcreatePath(path);
        this.setData(path, bytes);
    }

    public String rcreatePath(String path) throws ZKException {
        path = ZKUtil.normalize(path);
        if(this.exists(path)) return path;
        String[] splits = path.substring(1).split(ZKUtil.SEPARATOR);
        String p = "";
        for (String split : splits) {
            p = p + ZKUtil.SEPARATOR + split;
            if (!this.exists(p)) {
                this.createPath(p);
            }
        }
        return path;
    }

    public void createPath(String path) throws ZKException {
        this.createPath(path, true);
    }

    public void createPath(String path, boolean isPersistent) throws ZKException {
        CreateMode createMode = isPersistent ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
        path = ZKUtil.normalize(path);
        try {
            if(!this.exists(path)) {
                zookeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, createMode);
            }
        } catch (KeeperException e) {
            if(e instanceof KeeperException.NodeExistsException) {
            } else if(e instanceof KeeperException.NoNodeException) {
                throw new ZKException("The node ["  + e.getPath() + "]'s parent node doesn't exist,can't create it. ", e);
            } else {
                throw new ZKException("Other error", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ZKException(e);
        } catch(Throwable e){
            throw new ZKException(e);
        }
    }

    public boolean createPathIfAbsent(String path, byte[] bytes, boolean isPersistent) {
        CreateMode createMode = isPersistent ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
        path = ZKUtil.normalize(path);
        try {
            zookeeper.create(path, bytes, Ids.OPEN_ACL_UNSAFE, createMode);
        } catch (KeeperException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch(Throwable e){
            return false;
        }
        return true;
    }

    public boolean exists(String path) throws ZKException {
        return this.exists(path, (Watcher)null);
    }

    public boolean exists(String path,Watcher watcher) throws ZKException {
        path = ZKUtil.normalize(path);
        try {
            Stat stat = zookeeper.exists(path, watcher);
            return stat != null;
        } catch (KeeperException e) {
            throw new ZKException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ZKException(e);
        }catch(Throwable e) {
            throw new ZKException(e);
        }
    }

    public byte[] getData(String path)throws ZKException {
        return this.getData(path, (Watcher)null);
    }

    public byte[] getData(String path,Watcher watcher)throws ZKException {
        path = ZKUtil.normalize(path);
        byte[] data = null;
        try {
            data = zookeeper.getData(path, watcher, null);
        } catch (KeeperException e) {
            if(e instanceof KeeperException.NoNodeException){
                throw new ZKException("Node does not exist,path is [" + e.getPath() + "].",e);
            }else{
                throw new ZKException(e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ZKException(e);
        }
        return data;
    }

    public List<String> getChildren(String path)throws ZKException {
        return this.getChildren(path, (Watcher)null);
    }

    public List<String> getChildren(String path,Watcher watcher)throws ZKException {
        path = ZKUtil.normalize(path);
        List<String> children = null;
            try {
                children = zookeeper.getChildren(path, watcher);
            } catch (KeeperException e) {
                if(e instanceof KeeperException.NoNodeException) {
                    throw new ZKException("Node does not exist,path is [" + e.getPath() + "].",e);
                } else {
                    throw new ZKException(e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ZKException(e);
            }
        return children;
    }

    public void setData(String path,byte[] bytes)throws ZKException {
        this.setData(path, bytes, -1);
    }

    public void setData(String path,byte[] bytes,int version)throws ZKException {
        path = ZKUtil.normalize(path);
        try {
            zookeeper.setData(path, bytes, version);
        } catch (KeeperException e) {
            if(e instanceof KeeperException.NoNodeException) {
                this.rcreate(path, bytes);
            } else if(e instanceof KeeperException.BadVersionException) {
                throw new ZKException("Bad Version,path [" + e.getPath() +"] version [" + version+"],the given version does not match the node's version", e);
            } else {
                throw new ZKException("May be value(byte[]) is larger than 1MB.",e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ZKException(e);
        }
    }

    public boolean delete(String path)throws ZKException {
        path = ZKUtil.normalize(path);
        try {
            if (exists(path)) zookeeper.delete(path, -1);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ZKException("UnKnown", e);
        } catch (KeeperException e) {
            if(e instanceof KeeperException.NoNodeException){
                throw new ZKException("Node does not exist,path is [" + e.getPath() + "].",e);
            }else if(e instanceof KeeperException.NotEmptyException){
                throw new ZKException("The node has children,can't delete it.",e);
            }else{
                throw new ZKException("UnKnown.",e);
            }
        } catch(Throwable e){
            throw new ZKException(e);
        }
    }

    public void rdelete(String path) throws ZKException {
        path = ZKUtil.normalize(path);
        try {
            if (exists(path)) zookeeper.delete(path, -1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ZKException(e);
        } catch (KeeperException e) {
            if(e instanceof KeeperException.NotEmptyException) {
                    List<String> children = null;
                    try {
                        children = zookeeper.getChildren(path, false);
                    } catch (KeeperException e1) {
                        if(e1 instanceof KeeperException.NoNodeException) {
                            throw new ZKException("Node does not exist,path is [" + e.getPath() + "].",e);
                        }
                    } catch (InterruptedException e1) {
                        throw new ZKException(e);
                    }
                    for(String child : children) {
                        String _path = path + ZKUtil.SEPARATOR + child;
                        this.rdelete(_path);
                    }
                    this.rdelete(path);
            } else if(e instanceof KeeperException.NoNodeException) {
                throw new ZKException("Node does not exist,path is [" + e.getPath() + "].",e);
            }
        }
    }

}

