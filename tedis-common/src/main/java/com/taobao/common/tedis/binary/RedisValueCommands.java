/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.binary;

import java.util.List;
import java.util.Map;

import com.taobao.common.tedis.config.Process;
import com.taobao.common.tedis.config.ShardKey;
import com.taobao.common.tedis.config.Process.Policy;
import com.taobao.common.tedis.config.ShardKey.Type;


/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 上午11:41:46
 * @version 1.0
 */
public interface RedisValueCommands {

    @Process(Policy.READ)
    byte[] get(@ShardKey byte[] key);

    @Process(Policy.WRITE)
    byte[] getSet(@ShardKey byte[] key, byte[] value);

    @Process(Policy.READ)
    List<byte[]> mGet(@ShardKey(Type.MULTI) byte[]... keys);

    @Process(Policy.WRITE)
    Boolean set(@ShardKey byte[] key, byte[] value);

    @Process(Policy.WRITE)
    Boolean setNX(@ShardKey byte[] key, byte[] value);

    @Process(Policy.WRITE)
    Boolean setEx(@ShardKey byte[] key, long seconds, byte[] value);

    @Process(Policy.WRITE)
    Boolean mSet(@ShardKey(Type.MAP) Map<byte[], byte[]> tuple);

    @Process(Policy.WRITE)
    Boolean mSetNX(@ShardKey(Type.MAP) Map<byte[], byte[]> tuple);

    @Process(Policy.WRITE)
    Long incr(@ShardKey byte[] key);

    @Process(Policy.WRITE)
    Long incrBy(@ShardKey byte[] key, long value);

    @Process(Policy.WRITE)
    Long decr(@ShardKey byte[] key);

    @Process(Policy.WRITE)
    Long decrBy(@ShardKey byte[] key, long value);

    @Process(Policy.WRITE)
    Long append(@ShardKey byte[] key, byte[] value);

    @Process(Policy.READ)
    byte[] getRange(@ShardKey byte[] key, long begin, long end);

    @Process(Policy.WRITE)
    Long setRange(@ShardKey byte[] key, byte[] value, long offset);

    @Process(Policy.READ)
    Boolean getBit(@ShardKey byte[] key, long offset);

    @Process(Policy.WRITE)
    Long setBit(@ShardKey byte[] key, long offset, boolean value);

    @Process(Policy.READ)
    Long strLen(@ShardKey byte[] key);

}
