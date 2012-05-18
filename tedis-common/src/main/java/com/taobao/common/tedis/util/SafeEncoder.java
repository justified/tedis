/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.util;

import java.io.UnsupportedEncodingException;

import com.taobao.common.tedis.TedisDataException;
import com.taobao.common.tedis.TedisException;

public class SafeEncoder {
    public static byte[] encode(final String str) {
        try {
            if (str == null) {
                throw new TedisDataException("value sent to redis cannot be null");
            }
            return str.getBytes(Protocol.CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new TedisException(e);
        }
    }

    public static String encode(final byte[] data) {
        try {
            return new String(data, Protocol.CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new TedisException(e);
        }
    }
}
