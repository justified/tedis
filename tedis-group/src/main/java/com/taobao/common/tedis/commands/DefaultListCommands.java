/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.commands;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.taobao.common.tedis.TedisException;
import com.taobao.common.tedis.binary.RedisCommands;
import com.taobao.common.tedis.binary.RedisListCommands.Position;
import com.taobao.common.tedis.core.BaseCommands;
import com.taobao.common.tedis.core.ListCommands;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-28 10:04:45
 * @version 1.0
 */
public class DefaultListCommands extends BaseCommands implements ListCommands {

    protected RedisCommands redisCommands;

    public DefaultListCommands() {
    }

    public DefaultListCommands(RedisCommands redisCommands) {
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
    public <K, V> V index(final int namespace, final K key, final long index) {
        return deserializeValue((byte[])doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.lIndex(rawKey(namespace, key), index);
            }
        }));
    }

    @Override
    public <K, V> V leftPop(final int namespace, final K key) {
        return deserializeValue((byte[])doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.lPop(rawKey(namespace, key));
            }
        }));
    }

    @Override
    public <K, V> V leftPop(final int namespace, final K key, final long timeout, final TimeUnit unit) {
        final int tm = (int) unit.toSeconds(timeout);
        return deserializeValue((byte[])doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.bLPop(tm, rawKey(namespace, key));
            }
        }));
    }

    @Override
    public <K, V> Long leftPush(final int namespace, final K key, final V... value) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.lPush(rawKey(namespace, key), rawValues(value));
            }
        });
    }

    @Override
    public <K, V> Long leftInsert(final int namespace, final K key, final V pivot, final V value) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.lInsert(rawKey(namespace, key), Position.BEFORE, rawValue(pivot), rawValue(value));
            }
        });
    }

    @Override
    public <K, V> Long leftPushIfPresent(final int namespace, final K key, final V value) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.lPushX(rawKey(namespace, key), rawValue(value));
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> List<V> range(final int namespace, final K key, final long start, final long end) {
        return deserializeValues((List<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.lRange(rawKey(namespace, key), start, end);
            }
        }));
    }

    @Override
    public <K, V> Long remove(final int namespace, final K key, final long i, final Object value) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.lRem(rawKey(namespace, key), i, rawValue(value));
            }
        });
    }

    @Override
    public <K, V> V rightPop(final int namespace, final K key) {
        return deserializeValue((byte[])doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.rPop(rawKey(namespace, key));
            }
        }));
    }

    @Override
    public <K, V> V rightPop(final int namespace, final K key, final long timeout, final TimeUnit unit) {
        final int tm = (int) unit.toSeconds(timeout);
        return deserializeValue((byte[])doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.bRPop(tm, rawKey(namespace, key));
            }
        }));
    }

    @Override
    public <K, V> V rightPopAndLeftPush(final int namespace, final K sourceKey, final K destinationKey) {
        return deserializeValue((byte[])doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.rPopLPush(rawKey(namespace, sourceKey), rawKey(namespace, destinationKey));
            }
        }));
    }

    @Override
    public <K, V> V rightPopAndLeftPush(final int namespace, final K sourceKey, final K destinationKey, long timeout, TimeUnit unit) {
        final int tm = (int) unit.toSeconds(timeout);
        return deserializeValue((byte[])doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.bRPopLPush(tm, rawKey(namespace, sourceKey), rawKey(namespace, destinationKey));
            }
        }));
    }

    @Override
    public <K, V> Long rightPush(final int namespace, final K key, final V... value) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.rPush(rawKey(namespace, key), rawValues(value));
            }
        });
    }

    @Override
    public <K, V> Long rightInsert(final int namespace, final K key, final V pivot, final V value) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.lInsert(rawKey(namespace, key), Position.AFTER, rawValue(pivot),  rawValue(value));
            }
        });
    }

    @Override
    public <K, V> Long rightPushIfPresent(final int namespace, final K key, final V value) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.rPushX(rawKey(namespace, key), rawValue(value));
            }
        });
    }

    @Override
    public <K, V> void set(final int namespace, final K key, final long index, final V value) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.lSet(rawKey(namespace, key), index, rawValue(value));
                return null;
            }
        });
    }

    @Override
    public <K, V> Long size(final int namespace, final K key) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.lLen(rawKey(namespace, key));
            }
        });
    }

    @Override
    public <K, V> void trim(final int namespace, final K key, final long start, final long end) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.lTrim(rawKey(namespace, key), start, end);
                return null;
            }
        });
    }

}
