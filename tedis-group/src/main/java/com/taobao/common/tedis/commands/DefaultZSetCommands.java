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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.taobao.common.tedis.TedisException;
import com.taobao.common.tedis.binary.RedisCommands;
import com.taobao.common.tedis.binary.RedisZSetCommands.Tuple;
import com.taobao.common.tedis.core.BaseCommands;
import com.taobao.common.tedis.core.ZSetCommands;

public class DefaultZSetCommands extends BaseCommands implements ZSetCommands {

    private RedisCommands redisCommands;

    public <K, V> DefaultZSetCommands() {
    }

    public <K, V> DefaultZSetCommands(RedisCommands redisCommands) {
        this.redisCommands = redisCommands;
    }

    public <K, V> RedisCommands getRedisCommands() {
        return redisCommands;
    }

    public <K, V> void setRedisCommands(RedisCommands redisCommands) {
        this.redisCommands = redisCommands;
    }

    public <K, V> void init() {
        if (commandsProvider == null) {
            throw new TedisException("commandsProvider is null.please set a commandsProvider first.");
        }
        this.redisCommands = commandsProvider.getTedis();
    }

    @Override
    public <K, V> Boolean add(final int namespace, final K key, final V value, final double score) {
        return (Boolean)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zAdd(rawKey(namespace,key), score, rawValue(value));
            }
        });
    }

    @Override
    public <K, V> Long add(final int namespace, final K key, final Map<V, Double> maps) {
        final Tuple[] tuples = new Tuple[maps.size()];
        int i = 0;
        for(Entry<V, Double> m : maps.entrySet()) {
            tuples[i++] = new Tuple(rawValue(m.getKey()), m.getValue());
        }
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zAdd(rawKey(namespace,key), tuples);
            }
        });
    }

    @Override
    public <K, V> Long count(final int namespace, final K key, final double min, final double max) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zCount(rawKey(namespace,key), min, max);
            }
        });
    }

    @Override
    public <K, V> Double incrementScore(final int namespace, final K key, final V value, final double delta) {
        return (Double)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zIncrBy(rawKey(namespace,key), delta, rawValue(value));
            }
        });
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
                return commands.zInterStore(rawKey(namespace,destKey), rawKeys(namespace, key, otherKeys));
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Set<V> range(final int namespace, final K key, final long start, final long end) {
        return deserializeValues((Set<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zRange(rawKey(namespace,key), start, end);
            }
        }));
    }

    @SuppressWarnings("unchecked")
	@Override
	public <K, V> Map<V, Double> rangeWithScore(final int namespace, final K key, final long start, final long end) {
		return deserializeTruble((Set<Tuple>) doInTedis(namespace, new TedisBlock(redisCommands) {
			@Override
			public Object execute() {
				return commands.zRangeWithScore(rawKey(namespace, key), start, end);
			}
		}));
	}

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Set<V> rangeByScore(final int namespace, final K key, final double min, final double max) {
        return deserializeValues((Set<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zRangeByScore(rawKey(namespace,key), min, max);
            }
        }));
    }

	@SuppressWarnings("unchecked")
	@Override
	public <K, V> Set<V> rangeByScore(final int namespace, final K key, final double min, final double max, final int offset, final int count) {
		return deserializeValues((Set<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zRangeByScore(rawKey(namespace,key), min, max, offset, count);
            }
        }));
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K, V> Map<V, Double> rangeByScoreWithScore(final int namespace, final K key, final double min, final double max) {
		return deserializeTruble((Set<Tuple>) doInTedis(namespace, new TedisBlock(redisCommands) {
			@Override
			public Object execute() {
				return commands.zRangeByScoreWithScore(rawKey(namespace, key), min, max);
			}
		}));
	}

    @Override
    public <K, V> Long rank(final int namespace, final K key, final Object o) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zRank(rawKey(namespace,key), rawValue(o));
            }
        });
    }

    @Override
    public <K, V> Long remove(final int namespace, final K key, final Object... o) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zRem(rawKey(namespace,key), rawValues(o));
            }
        });
    }

    @Override
    public <K, V> void removeRange(final int namespace, final K key, final long start, final long end) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zRemRange(rawKey(namespace,key), start, end);
            }
        });
    }

    @Override
    public <K, V> void removeRangeByScore(final int namespace, final K key, final double min, final double max) {
        doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zRemRangeByScore(rawKey(namespace,key), min, max);
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Set<V> reverseRange(final int namespace, final K key, final long start, final long end) {
        return deserializeValues((Set<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zRevRange(rawKey(namespace,key), start, end);
            }
        }));
    }

	@SuppressWarnings("unchecked")
	@Override
	public <K, V> Set<V> reverseRangeByScore(final int namespace, final K key, final double min, final double max) {
		return deserializeValues((Set<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zRevRangeByScore(rawKey(namespace,key), min, max);
            }
        }));
	}

    @SuppressWarnings("unchecked")
	@Override
	public <K, V> Map<V, Double> reverseRangeWithScore(final int namespace, final K key, final long start, final long end) {
    	return deserializeTruble((Set<Tuple>) doInTedis(namespace, new TedisBlock(redisCommands) {
			@Override
			public Object execute() {
				return commands.zRevRangeWithScore(rawKey(namespace, key), start, end);
			}
		}));
	}

    @Override
    public <K, V> Long reverseRank(final int namespace, final K key, final Object o) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zRevRank(rawKey(namespace,key), rawValue(o));
            }
        });
    }

    @Override
    public <K, V> Double score(final int namespace, final K key, final Object o) {
        return (Double)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zScore(rawKey(namespace,key), rawValue(o));
            }
        });
    }

    @Override
    public <K, V> Long size(final int namespace, final K key) {
        return (Long)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zCard(rawKey(namespace,key));
            }
        });
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
                return commands.zUnionStore(rawKey(namespace,destKey), rawKeys(namespace, key, otherKeys));
            }
        });
    }

	@SuppressWarnings("unchecked")
	@Override
	public <K, V> Set<V> reverseRangeByScore(final int namespace, final K key, final double min, final double max, final int offset, final int count) {
		return deserializeValues((Set<byte[]>)doInTedis(namespace, new TedisBlock(redisCommands) {
            @Override
            public Object execute() {
                return commands.zRevRangeByScore(rawKey(namespace,key), min, max, offset, count);
            }
        }));
	}

}
