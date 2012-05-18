/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.atomic;

import com.taobao.common.tedis.core.BaseCommands;
import com.taobao.common.tedis.serializer.SerializationUtils;

public class ShardTest {

    private static byte[] getRouteFromKey(byte[] key) {
        int i = 0;
        while (key[i] != BaseCommands.PART[0]) {
            i++;
        }
        byte[] result = new byte[i];
        System.arraycopy(key, 0, result, 0, i);
        return result;
    }

    public static void main(String[] args) {
        byte[] bs = {'1','2',':', 'a'};
        System.out.println(Long.parseLong(SerializationUtils.deserialize(getRouteFromKey(bs))));
        long now = System.currentTimeMillis();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println(System.currentTimeMillis() - now);
    }

}
