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
import java.util.Set;

import com.taobao.common.tedis.TedisException;
import com.taobao.common.tedis.binary.RedisCommands;
import com.taobao.common.tedis.core.BaseCommands;
import com.taobao.common.tedis.core.SetCommands;

public class DefaultSetCommands extends BaseCommands implements SetCommands {

    private RedisCommands redisCommands;

    public DefaultSetCommands() {
    }

    public DefaultSetCommands(RedisCommands redisCommands) {
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
    public <K, V> Long add(final int namespace, final K key, final V... value) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.sAdd(rawKey(namespace, key), rawValues(value));
            }
        });
    }

    @Override
    public <K, V> Set<V> difference(final int namespace, K key, K otherKey) {
        return difference(namespace, key, Collections.singleton(otherKey));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Set<V> difference(final int namespace, final K key, final Collection<K> otherKeys) {
        return deserializeValues((Set<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.sDiff(rawKeys(namespace, key, otherKeys));
            }
        }));
    }

    @Override
    public <K, V> void differenceAndStore(final int namespace, K key, K otherKey, K destKey) {
        differenceAndStore(namespace, key, Collections.singleton(otherKey), destKey);
    }

    @Override
    public <K, V> void differenceAndStore(final int namespace, final K key, final Collection<K> otherKeys, final K destKey) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.sDiffStore(rawKey(namespace, destKey), rawKeys(namespace, key, otherKeys));
                return null;
            }
        });
    }

    @Override
    public <K, V> Set<V> intersect(final int namespace, final K key, final K otherKey) {
        return intersect(namespace, key, Collections.singleton(otherKey));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Set<V> intersect(final int namespace, final K key, final Collection<K> otherKeys) {
        return deserializeValues((Set<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.sInter(rawKeys(namespace, key, otherKeys));
            }
        }));
    }

    @Override
    public <K, V> void intersectAndStore(final int namespace, K key, K otherKey, K destKey) {
        intersectAndStore(namespace, key, Collections.singleton(otherKey), destKey);
    }

    @Override
    public <K, V> void intersectAndStore(final int namespace, final K key, final Collection<K> otherKeys, final K destKey) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.sInterStore(rawKey(namespace, destKey), rawKeys(namespace, key, otherKeys));
                return null;
            }
        });
    }

    @Override
    public <K, V> Boolean isMember(final int namespace, final K key, final Object o) {
        return (Boolean)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.sIsMember(rawKey(namespace, key), rawValue(o));
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Set<V> members(final int namespace, final K key) {
        return deserializeValues((Set<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.sMembers(rawKey(namespace, key));
            }
        }));
    }

    @Override
    public <K, V> Boolean move(final int namespace, final K key, final V value, final K destKey) {
        return (Boolean)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.sMove(rawKey(namespace, key), rawKey(namespace, destKey), rawValue(value));
            }
        });
    }

    @Override
    public <K, V> V pop(final int namespace, final K key) {
        return deserializeValue((byte[])doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.sPop(rawKey(namespace, key));
            }
        }));
    }

    @Override
    public <K, V> V randomMember(final int namespace, final K key) {
        return deserializeValue((byte[])doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.sRandMember(rawKey(namespace, key));
            }
        }));
    }

    @Override
    public <K, V> Long remove(final int namespace, final K key, final Object... o) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.sRem(rawKey(namespace, key), rawValues(o));
            }
        });
    }

    @Override
    public <K, V> Long size(final int namespace, final K key) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.sCard(rawKey(namespace, key));
            }
        });
    }

    @Override
    public <K, V> Set<V> union(final int namespace, final K key, final K otherKey) {
        return union(namespace, key, Collections.singleton(otherKey));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Set<V> union(final int namespace, final K key, final Collection<K> otherKeys) {
        return deserializeValues((Set<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.sUnion(rawKeys(namespace, key, otherKeys));
            }
        }));
    }

    @Override
    public <K, V> void unionAndStore(final int namespace, K key, K otherKey, K destKey) {
        unionAndStore(namespace, key, Collections.singleton(otherKey), destKey);
    }

    @Override
    public <K, V> void unionAndStore(final int namespace, final K key, final Collection<K> otherKeys, final K destKey) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.sUnionStore(rawKey(namespace, destKey), rawKeys(namespace, key, otherKeys));
                return null;
            }
        });
    }

}
