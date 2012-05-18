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
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.taobao.common.tedis.TedisException;
import com.taobao.common.tedis.binary.RedisCommands;
import com.taobao.common.tedis.cache.DefaultLocalCache;
import com.taobao.common.tedis.cache.LocalCache;
import com.taobao.common.tedis.core.BaseCommands;
import com.taobao.common.tedis.core.ValueCommands;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 11:34:39
 * @version 1.0
 */
public class DefaultValueCommands extends BaseCommands implements ValueCommands {

    protected RedisCommands redisCommands;

    @SuppressWarnings("rawtypes")
    protected LocalCache localCache;

    @SuppressWarnings("rawtypes")
    public DefaultValueCommands() {
        localCache = new DefaultLocalCache();
    }

    @SuppressWarnings("rawtypes")
    public DefaultValueCommands(RedisCommands redisCommands) {
        this.redisCommands = redisCommands;
        localCache = new DefaultLocalCache();
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
    public <K, V> V get(final int namespace, final K key) {
        return deserializeValue((byte[]) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.get(rawKey(namespace, key));
            }
        }));
    }


    @Override
    public <K, V> V getAndSet(final int namespace, final K key, final V value) {
        return deserializeValue((byte[]) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.getSet(rawKey(namespace, key), rawValue(value));
            }
        }));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> List<V> multiGet(final int namespace, final Collection<K> keys) {
        if (keys.isEmpty()) {
            return Collections.emptyList();
        }
        final byte[][] rawKeys = new byte[keys.size()][];
        int counter = 0;
        for (K hashKey : keys) {
            rawKeys[counter++] = rawKey(namespace, hashKey);
        }
        return deserializeValues((List<byte[]>) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.mGet(rawKeys);
            }
        }));
    }

    @Override
    public <K, V> void multiSet(final int namespace, final Map<? extends K, ? extends V> m) {
        if (m.isEmpty()) {
            return;
        }
        final Map<byte[], byte[]> rawKeys = new LinkedHashMap<byte[], byte[]>(m.size());
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            rawKeys.put(rawKey(namespace, entry.getKey()), rawValue(entry.getValue()));
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
    public <K, V> void multiSetIfAbsent(final int namespace, final Map<? extends K, ? extends V> m) {
        if (m.isEmpty()) {
            return;
        }

        final Map<byte[], byte[]> rawKeys = new LinkedHashMap<byte[], byte[]>(m.size());

        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            rawKeys.put(rawKey(namespace, entry.getKey()), rawValue(entry.getValue()));
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
    public <K, V> void set(final int namespace, final K key, final V value) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.set(rawKey(namespace, key), rawValue(value));
                return null;
            }
        });
    }

    @Override
    public <K, V> void set(final int namespace, final K key, final V value, final long timeout, final TimeUnit unit) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.setEx(rawKey(namespace, key), (int) unit.toSeconds(timeout), rawValue(value));
                return null;
            }
        });
    }



    @Override
    public <K, V> Boolean setIfAbsent(final int namespace, final K key, final V value) {
        return (Boolean)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.setNX(rawKey(namespace, key), rawValue(value));
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> V get(int namespace, K key, long timeout, TimeUnit unit) {
        V result = (V)localCache.get(key);
        if (result == null) {
            result = get(namespace, key);
            if (result != null) {
                localCache.put(key, result, (int)unit.toSeconds(timeout));
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> V get(int namespace, K key, Date expireAt) {
        V result = (V)localCache.get(key);
        if (result == null) {
            result = get(namespace, key);
            if (result != null) {
                localCache.put(key, result, expireAt);
            }
        }
        return result;
    }

    @Override
    public <K, V> List<V> multiGet(int namespace, Collection<K> keys, long timeout, TimeUnit unit) {
        // TODO MultiGet鏄畝鍗曞埄鐢ㄥ崟涓猤et缁勫悎,杩樻槸鏁存壒cache?濡備綍瑙ｅ喅鏁存壒cache鍛戒腑鐜囦綆鐨勯棶棰�
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> List<V> multiGet(int namespace, Collection<K> keys, Date expireAt) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

}
