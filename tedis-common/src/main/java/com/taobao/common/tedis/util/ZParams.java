/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.util;

import static com.taobao.common.tedis.util.Protocol.Keyword.AGGREGATE;
import static com.taobao.common.tedis.util.Protocol.Keyword.WEIGHTS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.taobao.common.tedis.util.Protocol;
import com.taobao.common.tedis.util.SafeEncoder;

public class ZParams {
    public enum Aggregate {
        SUM, MIN, MAX;

        public final byte[] raw;

        Aggregate() {
            raw = SafeEncoder.encode(name());
        }
    }

    private List<byte[]> params = new ArrayList<byte[]>();

    public ZParams weights(final int... weights) {
        params.add(WEIGHTS.raw);
        for (final int weight : weights) {
            params.add(Protocol.toByteArray(weight));
        }

        return this;
    }

    public Collection<byte[]> getParams() {
        return Collections.unmodifiableCollection(params);
    }

    public ZParams aggregate(final Aggregate aggregate) {
        params.add(AGGREGATE.raw);
        params.add(aggregate.raw);
        return this;
    }
}
