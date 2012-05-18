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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.taobao.common.tedis.Group;
import com.taobao.common.tedis.TedisException;
import com.taobao.common.tedis.binary.DataType;
import com.taobao.common.tedis.binary.RedisCommands;
import com.taobao.common.tedis.core.AtomicCommands;
import com.taobao.common.tedis.core.BaseCommands;
import com.taobao.common.tedis.core.HashCommands;
import com.taobao.common.tedis.core.ListCommands;
import com.taobao.common.tedis.core.SetCommands;
import com.taobao.common.tedis.core.StringCommands;
import com.taobao.common.tedis.core.TedisManager;
import com.taobao.common.tedis.core.ValueCommands;
import com.taobao.common.tedis.core.ZSetCommands;
import com.taobao.common.tedis.serializer.SerializationUtils;
import com.taobao.common.tedis.util.SortParams;

@SuppressWarnings("unchecked")
public class DefaultTedisManager extends BaseCommands implements TedisManager {

    private RedisCommands redisCommands;
    private DefaultAtomicCommands atomicCommands;
    private DefaultStringCommands stringCommands;
    private DefaultValueCommands valueCommands;
    private DefaultListCommands listCommands;
    private DefaultHashCommands hashCommands;
    private DefaultSetCommands setCommands;
    private DefaultZSetCommands zSetCommands;

    public DefaultTedisManager() {

    }

    public DefaultTedisManager(Group provider) {
        this.commandsProvider = provider;
        init();
    }

    public void setRedisCommands(Group provider) {
        this.commandsProvider = provider;
        init();
    }

    private void init() {
        if (commandsProvider == null) {
            throw new TedisException("commandsProvider is null.please set a commandsProvider first.");
        }
        this.redisCommands = commandsProvider.getTedis();
    }

    @Override
    public <K> void delete(final int namespace, final K key) {
        delete(namespace, Collections.singleton(key));
    }

    @Override
    public <K> void delete(final int namespace, final Collection<K> keys) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.del(rawKeys(namespace, keys));
                return null;
            }
        });
    }

    @Override
    public <K> Boolean expire(final int namespace, final K key, long timeout, TimeUnit unit) {
        final long seconds = unit.toSeconds(timeout);
        return (Boolean) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.expire(rawKey(namespace, key), seconds);
            }
        });
    }

    @Override
    public <K> Boolean expireAt(final int namespace, final K key, final Date date) {
        return (Boolean) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.expireAt(rawKey(namespace, key), date.getTime());
            }
        });
    }

    @Override
    public AtomicCommands getAtomicCommands() {
        if (atomicCommands == null) {
            atomicCommands = new DefaultAtomicCommands(this.redisCommands);
            initSerializer(atomicCommands);
        }
        return atomicCommands;
    }

    @Override
    public <K> Long getExpire(final int namespace, final K key) {
        return (Long) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.ttl(rawKey(namespace, key));
            }
        });
    }

    @Override
    public HashCommands getHashCommands() {
        if (hashCommands == null) {
            hashCommands = new DefaultHashCommands(this.redisCommands);
            initSerializer(hashCommands);
        }
        return hashCommands;
    }

    @Override
    public ListCommands getListCommands() {
        if (listCommands == null) {
            listCommands = new DefaultListCommands(this.redisCommands);
            initSerializer(listCommands);
        }
        return listCommands;
    }

    @Override
    public SetCommands getSetCommands() {
        if (setCommands == null) {
            setCommands = new DefaultSetCommands(this.redisCommands);
            initSerializer(setCommands);
        }
        return setCommands;
    }

    @Override
    public StringCommands getStringCommands() {
        if (stringCommands == null) {
            stringCommands = new DefaultStringCommands(this.redisCommands);
            initSerializer(stringCommands);
        }
        return stringCommands;
    }

    @Override
    public ValueCommands getValueCommands() {
        if (valueCommands == null) {
            valueCommands = new DefaultValueCommands(this.redisCommands);
            initSerializer(valueCommands);
        }
        return valueCommands;
    }

    @Override
    public ZSetCommands getZSetCommands() {
        if (zSetCommands == null) {
            zSetCommands = new DefaultZSetCommands(this.redisCommands);
            initSerializer(zSetCommands);
        }
        return zSetCommands;
    }

    private void initSerializer(BaseCommands baseCommands) {
        baseCommands.setKeySerializer(getKeySerializer());
        baseCommands.setValueSerializer(getValueSerializer());
        baseCommands.setStringSerializer(getStringSerializer());
        baseCommands.setHashKeySerializer(getHashKeySerializer());
        baseCommands.setHashValueSerializer(getHashValueSerializer());
    }

    @Override
    public <K> Boolean hasKey(final int namespace, final K key) {
        return (Boolean) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.exists(rawKey(namespace, key));
            }
        });
    }

    @Override
    public <K> Set<K> keys(final int namespace, final String pattern) {
        Set<byte[]> bytekeys = (Set<byte[]>) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.keys((namespace + ":" + pattern).getBytes());
            }
        });
        Set<byte[]> newbytekeys = new HashSet<byte[]>();
        for (byte[] bytekey : bytekeys) {
            newbytekeys.add(removeNamespaceFromKey(bytekey));
        }
        return SerializationUtils.deserialize(newbytekeys, getKeySerializer());
    }

    @Override
    public <K> Boolean persist(final int namespace, final K key) {
        return (Boolean) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.persist(rawKey(namespace, key));
            }
        });
    }

    @Override
    public <K> void rename(final int namespace, final K oldKey, final K newKey) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                commands.rename(rawKey(namespace, oldKey), rawKey(namespace, newKey));
                return null;
            }
        });
    }

    @Override
    public <K> Boolean renameIfAbsent(final int namespace, final K oldKey, final K newKey) {
        return (Boolean) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.renameNX(rawKey(namespace, oldKey), rawKey(namespace, newKey));
            }
        });
    }

    @Override
    public <K, V> List<V> sort(final int namespace, final K key, final SortParams params) {
        return deserializeValues((List<byte[]>) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.sort(rawKey(namespace, key), params);
            }
        }));
    }

    @Override
    public <K> Long sort(final int namespace, final K key, final SortParams params, final K storeKey) {
        return (Long) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.sort(rawKey(namespace, key), params, rawKey(namespace, storeKey));
            }
        });
    }

    @Override
    public <K> DataType type(final int namespace, final K key) {
        return DataType.fromCode((String) doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.type(rawKey(namespace, key));
            }
        }));
    }

    public Group getProvider() {
        return this.commandsProvider;
    }

    @Override
    public void destroy() {
        if (this.commandsProvider != null) {
            this.commandsProvider.destroy();
        }
    }

}
