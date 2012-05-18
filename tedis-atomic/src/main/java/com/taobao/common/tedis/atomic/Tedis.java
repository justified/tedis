/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.atomic;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.taobao.common.tedis.TedisException;
import com.taobao.common.tedis.binary.RedisCommands;
import com.taobao.common.tedis.config.Process;
import com.taobao.common.tedis.config.Process.Policy;
import com.taobao.common.tedis.config.ShardKey;
import com.taobao.common.tedis.util.Protocol;
import com.taobao.common.tedis.util.SafeEncoder;
import com.taobao.common.tedis.util.SortParams;
import com.taobao.common.tedis.util.TedisByteHashMap;
import com.taobao.common.tedis.util.ZParams;

public class Tedis implements RedisCommands {
    public static final int DEFAULT_PORT = 6379;
    public static final int DEFAULT_TIMEOUT = 2000;
    public static final String OK = "OK";
    public static final String PONG = "PONG";

    private final Client client;

    public Tedis(String host) {
        this(host, DEFAULT_PORT);
    }

    public Tedis(String host, int port) {
        this(host, port, DEFAULT_TIMEOUT);
    }

    public Tedis(String host, int port, int timeout) {
        this.client = new Client(host, port);
        this.client.setTimeout(timeout);
    }

    @Override
    public List<byte[]> sort(byte[] key, SortParams params) {
        try {
            if (params == null) {
                client.sort(key);
            } else {
                client.sort(key, params);
            }
            return client.getBinaryMultiBulkReply();
        } catch (Exception ex) {
            throw new TedisException(ex);
        }
    }

    @Override
    public Long sort(byte[] key, SortParams params, byte[] sortKey) {
        if (params == null) {
            client.sort(key, sortKey);
        } else {
            client.sort(key, params, sortKey);
        }
        return client.getIntegerReply();
    }

    @Override
    public byte[] echo(byte[] message) {
        client.echo(message);
        return client.getBinaryBulkReply();
    }

    @Override
    public Boolean ping() {
        client.ping();
        return PONG.equals(client.getStatusCodeReply());
    }

    @Override
    public Long del(byte[]... keys) {
        client.del(keys);
        return client.getIntegerReply();
    }

    @Override
    public Boolean exists(byte[] key) {
        client.exists(key);
        return client.getIntegerReply() == 1;
    }

    @Override
    public Boolean expire(byte[] key, long seconds) {
        client.expire(key, seconds);
        return client.getIntegerReply() == 1;
    }

    @Override
    public Boolean expireAt(byte[] key, long unixTime) {
        client.expireAt(key, unixTime);
        return client.getIntegerReply() == 1;
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        client.keys(pattern);
        return new HashSet<byte[]>(client.getBinaryMultiBulkReply());
    }

    @Override
    public Boolean persist(byte[] key) {
        client.persist(key);
        return client.getIntegerReply() == 1;

    }

    @Override
    public Boolean move(byte[] key, int dbIndex) {
        client.move(key, dbIndex);
        return client.getIntegerReply() == 1;
    }

    @Override
    public byte[] randomKey() {
        client.randomKey();
        return client.getBinaryBulkReply();
    }

    @Override
    public Boolean rename(byte[] oldName, byte[] newName) {
        client.rename(oldName, newName);
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public Boolean renameNX(byte[] oldName, byte[] newName) {
        client.renamenx(oldName, newName);
        return OK.equals(client.getIntegerReply());
    }

    @Override
    public Boolean select(int dbIndex) {
        client.select(dbIndex);
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public Long ttl(byte[] key) {
        client.ttl(key);
        return client.getIntegerReply();
    }

    @Override
    public String type(byte[] key) {
        client.type(key);
        return client.getStatusCodeReply();
    }

    @Override
    public byte[] get(byte[] key) {
        client.get(key);
        return client.getBinaryBulkReply();
    }

    @Override
    public Boolean set(byte[] key, byte[] value) {
        client.set(key, value);
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        client.getSet(key, value);
        return client.getBinaryBulkReply();
    }

    @Override
    public Long append(byte[] key, byte[] value) {
        client.append(key, value);
        return client.getIntegerReply();
    }

    @Override
    public List<byte[]> mGet(byte[]... keys) {
        client.mget(keys);
        return client.getBinaryMultiBulkReply();
    }

    @Override
    public Boolean mSet(Map<byte[], byte[]> tuples) {
        client.mset(convert(tuples));
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public Boolean mSetNX(Map<byte[], byte[]> tuples) {
        client.msetnx(convert(tuples));
        return client.getIntegerReply() == 1;
    }

    private byte[][] convert(Map<byte[], byte[]> hgetAll) {
        byte[][] result = new byte[hgetAll.size() * 2][];

        int index = 0;
        for (Map.Entry<byte[], byte[]> entry : hgetAll.entrySet()) {
            result[index++] = entry.getKey();
            result[index++] = entry.getValue();
        }
        return result;
    }

    @Override
    public Boolean setEx(byte[] key, long time, byte[] value) {
        client.setex(key, time, value);
        return OK.equals(client.getStatusCodeReply());

    }

    @Override
    public Boolean setNX(byte[] key, byte[] value) {
        client.setnx(key, value);
        return client.getIntegerReply() == 1;

    }

    @Override
    public byte[] getRange(byte[] key, long start, long end) {
        client.getrange(key, start, end);
        return client.getBinaryBulkReply();
    }

    @Override
    public Long decr(byte[] key) {
        client.decr(key);
        return client.getIntegerReply();
    }

    @Override
    public Long decrBy(byte[] key, long value) {
        client.decrBy(key, value);
        return client.getIntegerReply();
    }

    @Override
    public Long incr(byte[] key) {
        client.incr(key);
        return client.getIntegerReply();
    }

    @Override
    public Long incrBy(byte[] key, long value) {
        client.incrBy(key, value);
        return client.getIntegerReply();
    }

    @Override
    public Boolean getBit(byte[] key, long offset) {
        client.getbit(key, offset);
        return client.getIntegerReply() == 1;
    }

    @Override
    public Long setBit(byte[] key, long offset, boolean value) {
        client.setbit(key, offset, Protocol.toByteArray(value ? 1 : 0));
        return client.getIntegerReply();
    }

    @Override
    public Long setRange(byte[] key, byte[] value, long start) {
        client.setrange(key, start, value);
        return client.getIntegerReply();
    }

    @Override
    public Long strLen(byte[] key) {
        client.strlen(key);
        return client.getIntegerReply();
    }

    @Override
    public Long lPush(byte[] key, byte[]... value) {
        client.lpush(key, value);
        return client.getIntegerReply();
    }

    @Override
    public Long rPush(byte[] key, byte[]... value) {
        client.rpush(key, value);
        return client.getIntegerReply();
    }

    @Override
    public List<byte[]> bLPop(int timeout, byte[]... keys) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (final byte[] arg : keys) {
            args.add(arg);
        }
        args.add(Protocol.toByteArray(timeout));

        client.blpop(args.toArray(new byte[args.size()][]));
        client.setTimeoutInfinite();
        final List<byte[]> multiBulkReply = client.getBinaryMultiBulkReply();
        client.rollbackTimeout();
        return multiBulkReply;
    }

    @Override
    public List<byte[]> bRPop(int timeout, byte[]... keys) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (final byte[] arg : keys) {
            args.add(arg);
        }
        args.add(Protocol.toByteArray(timeout));

        client.brpop(args.toArray(new byte[args.size()][]));
        client.setTimeoutInfinite();
        final List<byte[]> multiBulkReply = client.getBinaryMultiBulkReply();
        client.rollbackTimeout();

        return multiBulkReply;
    }

    @Override
    public byte[] lIndex(byte[] key, long index) {
        client.lindex(key, index);
        return client.getBinaryBulkReply();
    }

    @Override
    public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
        client.linsert(key, where, pivot, value);
        return client.getIntegerReply();
    }

    @Override
    public Long lLen(byte[] key) {
        client.llen(key);
        return client.getIntegerReply();
    }

    @Override
    public byte[] lPop(byte[] key) {
        client.lpop(key);
        return client.getBinaryBulkReply();
    }

    @Override
    public List<byte[]> lRange(byte[] key, long start, long end) {
        client.lrange(key, start, end);
        return client.getBinaryMultiBulkReply();
    }

    @Override
    public Long lRem(byte[] key, long count, byte[] value) {
        client.lrem(key, count, value);
        return client.getIntegerReply();
    }

    @Override
    public Boolean lSet(byte[] key, long index, byte[] value) {
        client.lset(key, index, value);
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public Boolean lTrim(byte[] key, long start, long end) {
        client.ltrim(key, start, end);
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public byte[] rPop(byte[] key) {
        client.rpop(key);
        return client.getBinaryBulkReply();
    }

    @Override
    public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
        client.rpoplpush(srcKey, dstKey);
        return client.getBinaryBulkReply();
    }

    @Override
    public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
        client.brpoplpush(srcKey, dstKey, timeout);
        client.setTimeoutInfinite();
        byte[] reply = client.getBinaryBulkReply();
        client.rollbackTimeout();
        return reply;
    }

    @Override
    public Long lPushX(byte[] key, byte[] value) {
        client.lpushx(key, value);
        return client.getIntegerReply();
    }

    @Override
    public Long rPushX(byte[] key, byte[] value) {
        client.rpushx(key, value);
        return client.getIntegerReply();
    }

    @Override
    public Long sAdd(byte[] key, byte[]... value) {
        client.sadd(key, value);
        return client.getIntegerReply();
    }

    @Override
    public Long sCard(byte[] key) {
        client.scard(key);
        return client.getIntegerReply();
    }

    @Override
    public Set<byte[]> sDiff(byte[]... keys) {
        client.sdiff(keys);
        final List<byte[]> members = client.getBinaryMultiBulkReply();
        return new HashSet<byte[]>(members);
    }

    @Override
    public Long sDiffStore(byte[] destKey, byte[]... keys) {
        client.sdiffstore(destKey, keys);
        return client.getIntegerReply();
    }

    @Override
    public Set<byte[]> sInter(byte[]... keys) {
        client.sinter(keys);
        final List<byte[]> members = client.getBinaryMultiBulkReply();
        return new HashSet<byte[]>(members);
    }

    @Override
    public Long sInterStore(byte[] destKey, byte[]... keys) {
        client.sinterstore(destKey, keys);
        return client.getIntegerReply();
    }

    @Override
    public Boolean sIsMember(byte[] key, byte[] value) {
        client.sismember(key, value);
        return client.getIntegerReply() == 1;
    }

    @Override
    public Set<byte[]> sMembers(byte[] key) {
        client.smembers(key);
        final List<byte[]> members = client.getBinaryMultiBulkReply();
        return new HashSet<byte[]>(members);
    }

    @Override
    public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
        client.smove(srcKey, destKey, value);
        return client.getIntegerReply() == 1;
    }

    @Override
    public byte[] sPop(byte[] key) {
        client.spop(key);
        return client.getBinaryBulkReply();
    }

    @Override
    public byte[] sRandMember(byte[] key) {
        client.srandmember(key);
        return client.getBinaryBulkReply();
    }

    @Override
    public Long sRem(byte[] key, byte[]... value) {
        client.srem(key, value);
        return client.getIntegerReply();
    }

    @Override
    public Set<byte[]> sUnion(byte[]... keys) {
        client.sunion(keys);
        final List<byte[]> members = client.getBinaryMultiBulkReply();
        return new HashSet<byte[]>(members);
    }

    @Override
    public Long sUnionStore(byte[] destKey, byte[]... keys) {
        client.sunionstore(destKey, keys);
        return client.getIntegerReply();
    }

    @Override
    public Boolean zAdd(byte[] key, double score, byte[] value) {
        client.zadd(key, score, value);
        return client.getIntegerReply() == 1;
    }

    @Process(Policy.WRITE)
    public Long zAdd(@ShardKey byte[] key, Tuple... value) {
        client.zadd(key, value);
        return client.getIntegerReply();
    }

    @Override
    public Long zCard(byte[] key) {
        client.zcard(key);
        return client.getIntegerReply();
    }

    @Override
    public Long zCount(byte[] key, double min, double max) {
        client.zcount(key, min, max);
        return client.getIntegerReply();
    }

    @Override
    public Double zIncrBy(byte[] key, double increment, byte[] value) {
        client.zincrby(key, increment, value);
        String newscore = client.getBulkReply();
        return Double.valueOf(newscore);
    }

    @Override
    public Long zInterStore(byte[] destKey, ZParams params, int[] weights, byte[]... sets) {
        client.zinterstore(destKey, params, sets);
        return client.getIntegerReply();
    }

    @Override
    public Long zInterStore(byte[] destKey, byte[]... sets) {
        client.zinterstore(destKey, sets);
        return client.getIntegerReply();
    }

    @Override
    public Set<byte[]> zRange(byte[] key, long start, long end) {
        client.zrange(key, start, end);
        final List<byte[]> members = client.getBinaryMultiBulkReply();
        return new LinkedHashSet<byte[]>(members);
    }

    @Override
    public Set<Tuple> zRangeWithScore(byte[] key, long start, long end) {
        client.zrangeWithScores(key, start, end);
        Set<Tuple> set = getBinaryTupledSet();
        return set;
    }

    private Set<Tuple> getBinaryTupledSet() {
        List<byte[]> membersWithScores = client.getBinaryMultiBulkReply();
        Set<Tuple> set = new LinkedHashSet<Tuple>();
        Iterator<byte[]> iterator = membersWithScores.iterator();
        while (iterator.hasNext()) {
            set.add(new Tuple(iterator.next(), Double.valueOf(SafeEncoder.encode(iterator.next()))));
        }
        return set;
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
        client.zrangeByScore(key, min, max);
        return new LinkedHashSet<byte[]>(client.getBinaryMultiBulkReply());
    }

    @Override
    public Set<Tuple> zRangeByScoreWithScore(byte[] key, double min, double max) {
        client.zrangeByScoreWithScores(key, min, max);
        Set<Tuple> set = getBinaryTupledSet();
        return set;
    }

    @Override
    public Set<Tuple> zRevRangeWithScore(byte[] key, long start, long end) {
        client.zrevrangeWithScores(key, start, end);
        Set<Tuple> set = getBinaryTupledSet();
        return set;
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
        client.zrangeByScore(key, min, max, offset, count);
        return new LinkedHashSet<byte[]>(client.getBinaryMultiBulkReply());
    }

    @Override
    public Set<Tuple> zRangeByScoreWithScore(byte[] key, double min, double max, long offset, long count) {
        client.zrangeByScoreWithScores(key, min, max, offset, count);
        Set<Tuple> set = getBinaryTupledSet();
        return set;
    }

    @Override
    public Set<byte[]> zRevRangeByScore(@ShardKey byte[] key, double min, double max) {
        client.zrevrangeByScore(key, max, min);
        return new LinkedHashSet<byte[]>(client.getBinaryMultiBulkReply());
    }

    @Override
    public Set<Tuple> zRevRangeByScoreWithScore(@ShardKey byte[] key, double min, double max) {
        client.zrevrangeByScoreWithScores(key, max, min);
        Set<Tuple> set = getBinaryTupledSet();
        return set;
    }

    @Override
    public Set<byte[]> zRevRangeByScore(@ShardKey byte[] key, double min, double max, long offset, long count) {
        client.zrevrangeByScore(key, max, min, offset, count);
        return new LinkedHashSet<byte[]>(client.getBinaryMultiBulkReply());
    }

    @Override
    public Set<Tuple> zRevRangeByScoreWithScore(@ShardKey byte[] key, double min, double max, long offset, long count) {
        client.zrevrangeByScoreWithScores(key, max, min, offset, count);
        Set<Tuple> set = getBinaryTupledSet();
        return set;
    }

    @Override
    public Long zRank(byte[] key, byte[] value) {
        client.zrank(key, value);
        return client.getIntegerReply();
    }

    @Override
    public Long zRem(byte[] key, byte[]... value) {
        client.zrem(key, value);
        return client.getIntegerReply();
    }

    @Override
    public Long zRemRange(byte[] key, long start, long end) {
        client.zremrangeByRank(key, start, end);
        return client.getIntegerReply();
    }

    @Override
    public Long zRemRangeByScore(byte[] key, double min, double max) {
        client.zremrangeByScore(key, min, max);
        return client.getIntegerReply();
    }

    @Override
    public Set<byte[]> zRevRange(byte[] key, long start, long end) {
        client.zrevrange(key, start, end);
        final List<byte[]> members = client.getBinaryMultiBulkReply();
        return new LinkedHashSet<byte[]>(members);
    }

    @Override
    public Long zRevRank(byte[] key, byte[] value) {
        client.zrevrank(key, value);
        return client.getIntegerReply();
    }

    @Override
    public Double zScore(byte[] key, byte[] value) {
        client.zscore(key, value);
        final String score = client.getBulkReply();
        return (score != null ? new Double(score) : null);

    }

    @Override
    public Long zUnionStore(byte[] destKey, ZParams params, int[] weights, byte[]... sets) {
        client.zunionstore(destKey, params, sets);
        return client.getIntegerReply();
    }

    @Override
    public Long zUnionStore(byte[] destKey, byte[]... sets) {
        client.zunionstore(destKey, sets);
        return client.getIntegerReply();
    }

    @Override
    public Boolean hSet(byte[] key, byte[] field, byte[] value) {
        client.hset(key, field, value);
        return client.getIntegerReply() == 1;
    }

    @Override
    public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
        client.hsetnx(key, field, value);
        return client.getIntegerReply() == 1;
    }

    @Override
    public Long hDel(byte[] key, byte[]... field) {
        client.hdel(key, field);
        return client.getIntegerReply();
    }

    @Override
    public Boolean hExists(byte[] key, byte[] field) {
        client.hexists(key, field);
        return client.getIntegerReply() == 1;
    }

    @Override
    public byte[] hGet(byte[] key, byte[] field) {
        client.hget(key, field);
        return client.getBinaryBulkReply();
    }

    @Override
    public Map<byte[], byte[]> hGetAll(byte[] key) {
        client.hgetAll(key);
        final List<byte[]> flatHash = client.getBinaryMultiBulkReply();
        final Map<byte[], byte[]> hash = new TedisByteHashMap();
        final Iterator<byte[]> iterator = flatHash.iterator();
        while (iterator.hasNext()) {
            hash.put(iterator.next(), iterator.next());
        }

        return hash;
    }

    @Override
    public Long hIncrBy(byte[] key, byte[] field, long delta) {
        client.hincrBy(key, field, delta);
        return client.getIntegerReply();
    }

    @Override
    public Set<byte[]> hKeys(byte[] key) {
        client.hkeys(key);
        final List<byte[]> lresult = client.getBinaryMultiBulkReply();
        return new HashSet<byte[]>(lresult);
    }

    @Override
    public Long hLen(byte[] key) {
        client.hlen(key);
        return client.getIntegerReply();
    }

    @Override
    public List<byte[]> hMGet(byte[] key, byte[]... fields) {
        client.hmget(key, fields);
        return client.getBinaryMultiBulkReply();
    }

    @Override
    public Boolean hMSet(byte[] key, Map<byte[], byte[]> tuple) {
        client.hmset(key, tuple);
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public List<byte[]> hVals(byte[] key) {
        client.hvals(key);
        final List<byte[]> lresult = client.getBinaryMultiBulkReply();
        return lresult;
    }

    @Override
    public Boolean connect() {
        client.connect();
        return true;
    }

    public Boolean disconnect() {
        client.disconnect();
        return true;
    }

    public Boolean auth(String password) {
        client.auth(password);
        return OK.equals(client.getStatusCodeReply());
    }

    public Boolean quit() {
        client.quit();
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public Boolean bgWriteAof() {
        client.bgrewriteaof();
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public Boolean bgSave() {
        client.bgsave();
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public Long lastSave() {
        client.lastsave();
        return client.getIntegerReply();
    }

    @Override
    public Boolean save() {
        client.save();
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public Long dbSize() {
        client.dbSize();
        return client.getIntegerReply();
    }

    @Override
    public Boolean flushDb() {
        client.flushDB();
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public Boolean flushAll() {
        client.flushAll();
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public Properties info() {
        Properties info = new Properties();
        client.info();
        StringReader stringReader = new StringReader(client.getBulkReply());
        try {
            info.load(stringReader);
        } catch (Exception ex) {
            throw new TedisException("Cannot read Redis info", ex);
        } finally {
            stringReader.close();
        }
        return info;
    }

    @Override
    public Boolean shutdown() {
        client.shutdown();
        String status = null;
        try {
            status = client.getStatusCodeReply();
        } catch (Exception e) {
        }
        if (status == null) {
            return true;
        }
        return false;
    }

    @Override
    public List<String> getConfig(String pattern) {
        client.configGet(SafeEncoder.encode(pattern));
        return client.getMultiBulkReply();
    }

    @Override
    public Boolean setConfig(String param, String value) {
        client.configSet(SafeEncoder.encode(param), SafeEncoder.encode(value));
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public Boolean resetConfigStats() {
        client.configResetStat();
        return OK.equals(client.getStatusCodeReply());
    }

    @Override
    public Long getDB() {
        return client.getDB();
    }

}
