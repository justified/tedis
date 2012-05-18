/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.cache;

import java.util.List;
import java.util.Map;

public class CacheSizes {

    public CacheSizes() {
    }

    public static int sizeOfObject() {
        return 4;
    }

    public static int sizeOfString(String string) {
        if (string == null)
            return 0;
        else
            return 4 + string.length() * 2;
    }

    public static int sizeOfInt() {
        return 4;
    }

    public static int sizeOfChar() {
        return 2;
    }

    public static int sizeOfBoolean() {
        return 1;
    }

    public static int sizeOfLong() {
        return 8;
    }

    public static int sizeOfDouble() {
        return 8;
    }

    public static int sizeOfDate() {
        return 12;
    }

    @SuppressWarnings("rawtypes")
    public static int sizeOfMap(Map map) {
        if (map == null)
            return 0;
        int size = 36;
        Object values[] = map.values().toArray();
        for (int i = 0; i < values.length; i++)
            size += sizeOfString((String) values[i]);

        Object keys[] = map.keySet().toArray();
        for (int i = 0; i < keys.length; i++)
            size += sizeOfString((String) keys[i]);

        return size;
    }

    @SuppressWarnings("rawtypes")
    public static int sizeOfList(List list) {
        if (list == null)
            return 0;
        int size = 36;
        Object values[] = list.toArray();
        for (int i = 0; i < values.length; i++) {
            Object obj = values[i];
            if (obj instanceof String) {
                size += sizeOfString((String) obj);
                continue;
            }
            if (obj instanceof Long)
                size += sizeOfLong() + sizeOfObject();
            else
                size += ((Cacheable) obj).getCachedSize();
        }

        return size;
    }
}
