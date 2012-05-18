/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.commands;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.taobao.common.tedis.TedisException;
import com.taobao.common.tedis.binary.RedisCommands;
import com.taobao.common.tedis.core.AtomicCommands;
import com.taobao.common.tedis.core.BaseCommands;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-28 10:04:32
 * @version 1.0
 */
public class DefaultAtomicCommands extends BaseCommands implements AtomicCommands {

    protected RedisCommands redisCommands;

    public DefaultAtomicCommands() {
    }

    public DefaultAtomicCommands(RedisCommands redisCommands) {
        this.redisCommands = redisCommands;
    }

    public RedisCommands getRedisCommands() {
        return redisCommands;
    }

    public void setRedisCommands(RedisCommands redisCommands) {
        this.redisCommands = redisCommands;
    }

    public void init() {
        if (commandsProvider == null) {
            throw new TedisException("commandsProvider is null.please set a commandsProvider first.");
        }
        this.redisCommands = commandsProvider.getTedis();
    }

    @Override
    public long get(final int namespace, final Object key) {
        return deserializeLong((byte[]) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.get(rawKey(namespace, key));
            }
        }));
    }


    @Override
    public long getAndSet(final int namespace, final Object key, final long value) {
        return deserializeLong((byte[]) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.getSet(rawKey(namespace, key), rawLong(value));
            }
        }));
    }

    @Override
    public Long increment(final int namespace, final Object key, final long delta) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                byte[] rawKey = rawKey(namespace, key);
                if (delta == 1) {
                    return commands.incr(rawKey);
                }
                if (delta == -1) {
                    return commands.decr(rawKey);
                }
                if (delta < 0) {
                    return commands.decrBy(rawKey, delta);
                }
                return commands.incrBy(rawKey, delta);
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Long> multiGet(final int namespace, final Collection<? extends Object> keys) {
        if (keys.isEmpty()) {
            return Collections.emptyList();
        }
        final byte[][] rawKeys = new byte[keys.size()][];
        int counter = 0;
        for (Object hashKey : keys) {
            rawKeys[counter++] = rawKey(namespace, hashKey);
        }
        return deserializeLongs((List<byte[]>) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.mGet(rawKeys);
            }
        }));
    }

    @Override
    public void multiSet(final int namespace, final Map<? extends Object, Long> m) {
        if (m.isEmpty()) {
            return;
        }
        final Map<byte[], byte[]> rawKeys = new LinkedHashMap<byte[], byte[]>(m.size());
        for (Map.Entry<? extends Object, Long> entry : m.entrySet()) {
            rawKeys.put(rawKey(namespace, entry.getKey()), rawLong(entry.getValue()));
        }
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.mSet(rawKeys);
                return null;
            }
        });
    }

    @Override
    public void multiSetIfAbsent(final int namespace, final Map<? extends Object, Long> m) {
        if (m.isEmpty()) {
            return;
        }

        final Map<byte[], byte[]> rawKeys = new LinkedHashMap<byte[], byte[]>(m.size());

        for (Map.Entry<? extends Object, Long> entry : m.entrySet()) {
            rawKeys.put(rawKey(namespace, entry.getKey()), rawLong(entry.getValue()));
        }
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.mSetNX(rawKeys);
                return null;
            }
        });

    }

    @Override
    public void set(final int namespace, final Object key, final long value) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.set(rawKey(namespace, key), rawLong(value));
                return null;
            }
        });
    }

    @Override
    public void set(final int namespace, final Object key, final long value, final long timeout, final TimeUnit unit) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.setEx(rawKey(namespace, key), (int) unit.toSeconds(timeout), rawLong(value));
                return null;
            }
        });
    }

    @Override
    public Boolean setIfAbsent(final int namespace, final Object key, final long value) {
        return (Boolean)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.setNX(rawKey(namespace, key), rawLong(value));
            }
        });
    }
}
