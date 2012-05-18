/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.serializer;

import java.nio.charset.Charset;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 上午11:55:24
 * @version 1.0
 */
public class StringTedisSerializer implements TedisSerializer<String> {

    private final Charset charset;

    public StringTedisSerializer() {
        this(Charset.forName(SerializationUtils.CHARSET));
    }

    public StringTedisSerializer(Charset charset) {
        this.charset = charset;
    }

    @Override
    public String deserialize(byte[] bytes) {
        return (bytes == null ? null : new String(bytes, charset));
    }

    @Override
    public byte[] serialize(String string) {
        return (string == null ? null : string.getBytes(charset));
    }
}
