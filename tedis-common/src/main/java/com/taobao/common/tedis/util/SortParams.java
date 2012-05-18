/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.util;

import static com.taobao.common.tedis.util.Protocol.Keyword.ALPHA;
import static com.taobao.common.tedis.util.Protocol.Keyword.ASC;
import static com.taobao.common.tedis.util.Protocol.Keyword.BY;
import static com.taobao.common.tedis.util.Protocol.Keyword.DESC;
import static com.taobao.common.tedis.util.Protocol.Keyword.GET;
import static com.taobao.common.tedis.util.Protocol.Keyword.LIMIT;
import static com.taobao.common.tedis.util.Protocol.Keyword.NOSORT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.taobao.common.tedis.core.BaseCommands;
import com.taobao.common.tedis.serializer.StringTedisSerializer;

public class SortParams {
    private List<byte[]> params = new ArrayList<byte[]>();
    protected StringTedisSerializer stringSerializer = new StringTedisSerializer();

    public SortParams by(final int namespace, final String pattern) {
        return by(namespace, SafeEncoder.encode(pattern));
    }

    public SortParams by(final int namespace, final byte[] pattern) {
        params.add(BY.raw);
        params.add(rawKey(namespace, pattern));
        return this;
    }

    private byte[] rawKey(int namespace, byte[] bytekey) {
        byte[] prefix = stringSerializer.serialize(String.valueOf(namespace));
        byte[] result = new byte[prefix.length + bytekey.length + 1];
        System.arraycopy(prefix, 0, result, 0, prefix.length);
        System.arraycopy(BaseCommands.PART, 0, result, prefix.length, 1);
        System.arraycopy(bytekey, 0, result, prefix.length + 1, bytekey.length);
        return result;
    }

    public SortParams nosort() {
        params.add(BY.raw);
        params.add(NOSORT.raw);
        return this;
    }

    public Collection<byte[]> getParams() {
        return Collections.unmodifiableCollection(params);
    }

    public SortParams desc() {
        params.add(DESC.raw);
        return this;
    }

    public SortParams asc() {
        params.add(ASC.raw);
        return this;
    }

    public SortParams limit(final int start, final int count) {
        params.add(LIMIT.raw);
        params.add(Protocol.toByteArray(start));
        params.add(Protocol.toByteArray(count));
        return this;
    }

    public SortParams alpha() {
        params.add(ALPHA.raw);
        return this;
    }

    public SortParams get(int namespace, String... patterns) {
        for (final String pattern : patterns) {
            params.add(GET.raw);
            params.add(rawKey(namespace, SafeEncoder.encode(pattern)));
        }
        return this;
    }

    public SortParams get(int namespace, byte[]... patterns) {
        for (final byte[] pattern : patterns) {
            params.add(GET.raw);
            params.add(rawKey(namespace, pattern));
        }
        return this;
    }
}