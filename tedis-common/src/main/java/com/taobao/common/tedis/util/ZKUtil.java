/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ZKUtil {

    public static final String SEPARATOR = "/";
    public static final String LOCK_ROOT = "lock";
    public static final String MUTEXLOCK_ROOT = "lock/mutex";
    public static final String LOCK_OWNER = "owner";

    public static String normalize(String path) {
        String temp = path;
        if(!path.startsWith(SEPARATOR)) {
            temp = SEPARATOR + path;
        }
        if(path.endsWith(SEPARATOR)) {
            temp = temp.substring(0, temp.length()-1);
            return normalize(temp);
        }else {
            return temp;
        }
    }

    public static byte[] toBytes(String data) {
        if(data == null || data.trim().equals("")) return null;
        return data.getBytes();
    }

    public static String toString(byte[] bytes) {
        if(bytes == null || bytes.length ==0) return null;
        return new String(bytes);
    }

    public static byte[] toBytes(Serializable obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream objout = new ObjectOutputStream(out);
        objout.writeObject(obj);
        return out.toByteArray();
    }

    public static Object toObject(byte[] bytes) throws IOException, ClassNotFoundException{
        ObjectInputStream objin = new ObjectInputStream(new ByteArrayInputStream(bytes));
        return objin.readObject();
    }

    public static String contact(String path1,String path2){
        if(path2.startsWith(SEPARATOR)) {
            path2 = path2.substring(1);
        }
        if(path1.endsWith(SEPARATOR)) {
            return normalize(path1 + path2);
        } else {
            return normalize(path1 + SEPARATOR + path2);
        }
    }

}

