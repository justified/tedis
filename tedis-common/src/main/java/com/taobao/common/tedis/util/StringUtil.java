/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.util;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

public class StringUtil {

    private static String localIp = null;
    private static ReentrantLock lock = new ReentrantLock();

    public static final String getLocalHostIP() {
        try {
            lock.lock();
            if(StringUtil.isBlank(localIp)) {
                try {
                    localIp = InetAddress.getLocalHost().getHostAddress();
                } catch (UnknownHostException e) {
                    throw new RuntimeException("[local-ip] an exception occured when get local ip address", e);
                }
                StringBuffer sb = new StringBuffer();
                Random random = new Random();
                sb.append(localIp).append("-");
                for (int i = 0; i < 16; i++) {
                    sb.append(random.nextInt(10));
                }
                localIp = sb.toString();
            }
        } finally {
            lock.unlock();
        }
        return localIp;
    }

    /*
    public static final String getLocalHostIP() {
        try {
            lock.lock();
            localIp = Thread.currentThread().getName().substring(0, 1);
            StringBuffer sb = new StringBuffer();
            Random random = new Random();
            sb.append(localIp).append("-");
            for (int i = 0; i < 16; i++) {
                sb.append(random.nextInt(10));
            }
            localIp = sb.toString();
        } finally {
            lock.unlock();
        }
        return localIp;
    }
    */

    public static boolean isBlank(String str) {
        if (str == null || str.equals("")) return true;
        if (str.trim().equals("")) return true;
        return false;
    }

    public static boolean isLastConn(String owner) {
        getLocalHostIP();
        if (isBlank(owner)) return false;
        if (owner.indexOf('-') == -1) owner = owner.concat("-0");
        String localIpHeader = localIp.substring(0, localIp.indexOf('-'));
        String ownerHeader = owner.substring(0, owner.indexOf('-'));
        if (localIpHeader.equals(ownerHeader)) {
            String localIpRandom = localIp.substring(localIp.indexOf('-') + 1);
            String ownerRandom = owner.substring(owner.indexOf('-') + 1);
            return !localIpRandom.equals(ownerRandom);
        }
        return false;
    }

}

