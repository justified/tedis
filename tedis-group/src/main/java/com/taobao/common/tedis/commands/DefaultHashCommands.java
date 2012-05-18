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
import java.util.Set;

import com.taobao.common.tedis.TedisException;
import com.taobao.common.tedis.binary.RedisCommands;
import com.taobao.common.tedis.core.BaseCommands;
import com.taobao.common.tedis.core.HashCommands;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-28 上午10:04:38
 * @version 1.0
 */
public class DefaultHashCommands extends BaseCommands implements HashCommands {

    protected RedisCommands redisCommands;

    public DefaultHashCommands() {
    }

    public DefaultHashCommands(RedisCommands redisCommands) {
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
    public <H, HK, HV> void delete(final int namespace, final H key, final Object... hashKey) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.hDel(rawKey(namespace, key), rawHashKeys(hashKey));
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <H, HK, HV> Map<HK, HV> entries(final int namespace, final H key) {
        return deserializeHashMap((Map<byte[], byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.hGetAll(rawKey(namespace, key));
            }
        }));
    }

    @Override
    public <H, HK, HV> HV get(final int namespace, final H key, final Object hashKey) {
        return deserializeHashValue((byte[])doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.hGet(rawKey(namespace, key), rawHashKey(hashKey));
            }
        }));
    }

    @Override
    public <H, HK, HV> Boolean hasKey(final int namespace, final H key, final Object hashKey) {
        return (Boolean)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.hExists(rawKey(namespace, key), rawHashKey(hashKey));
            }
        });
    }

    @Override
    public <H, HK, HV> Long increment(final int namespace, final H key, final HK hashKey, final long delta) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.hIncrBy(rawKey(namespace, key), rawHashKey(hashKey), delta);
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <H, HK, HV> Set<HK> keys(final int namespace, final H key) {
        return deserializeHashKeys((Set<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.hKeys(rawKey(namespace, key));
            }
        }));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <H, HK, HV> Collection<HV> multiGet(final int namespace, H key, Collection<HK> hashKeys) {
        if (hashKeys == null || hashKeys.isEmpty()) {
            return Collections.emptyList();
        }

        final byte[] rawKey = rawKey(namespace, key);

        final byte[][] rawHashKeys = new byte[hashKeys.size()][];

        int counter = 0;
        for (HK hashKey : hashKeys) {
            rawHashKeys[counter++] = rawHashKey(hashKey);
        }
        return deserializeHashValues((List<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.hMGet(rawKey, rawHashKeys);
            }
        }));
    }

    @Override
    public <H, HK, HV> void put(final int namespace, final H key, final HK hashKey, final HV value) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.hSet(rawKey(namespace, key), rawHashKey(hashKey), rawHashValue(value));
            }
        });
    }

    @Override
    public <H, HK, HV> void putAll(final int namespace, H key, Map<? extends HK, ? extends HV> m) {
        if (m.isEmpty()) {
            return;
        }

        final byte[] rawKey = rawKey(namespace, key);

        final Map<byte[], byte[]> hashes = new LinkedHashMap<byte[], byte[]>(m.size());

        for (Map.Entry<? extends HK, ? extends HV> entry : m.entrySet()) {
            hashes.put(rawHashKey(entry.getKey()), rawHashValue(entry.getValue()));
        }
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.hMSet(rawKey, hashes);
                return null;
            }
        });
    }

    @Override
    public <H, HK, HV> Boolean putIfAbsent(final int namespace, final H key, final HK hashKey, final HV value) {
        return (Boolean)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.hSetNX(rawKey(namespace, key), rawHashKey(hashKey), rawHashValue(value));
            }
        });
    }

    @Override
    public <H, HK, HV> Long size(final int namespace, final H key) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.hLen(rawKey(namespace, key));
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <H, HK, HV> Collection<HV> values(final int namespace, final H key) {
        return deserializeHashValues((List<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.hVals(rawKey(namespace, key));
            }
        }));
    }

}
