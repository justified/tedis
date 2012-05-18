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
import java.util.Set;

import com.taobao.common.tedis.config.Process;
import com.taobao.common.tedis.config.ShardKey;
import com.taobao.common.tedis.config.Process.Policy;

/**
 *
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 09:08:49
 * @version 1.0
 */
public interface RedisHashCommands {

    @Process(Policy.WRITE)
    Boolean hSet(@ShardKey byte[] key, byte[] field, byte[] value);

    @Process(Policy.WRITE)
    Boolean hSetNX(@ShardKey byte[] key, byte[] field, byte[] value);

    @Process(Policy.READ)
    byte[] hGet(@ShardKey byte[] key, byte[] field);

    @Process(Policy.READ)
    List<byte[]> hMGet(@ShardKey byte[] key, byte[]... fields);

    @Process(Policy.WRITE)
    Boolean hMSet(@ShardKey byte[] key, Map<byte[], byte[]> hashes);

    @Process(Policy.WRITE)
    Long hIncrBy(@ShardKey byte[] key, byte[] field, long delta);

    @Process(Policy.READ)
    Boolean hExists(@ShardKey byte[] key, byte[] field);

    @Process(Policy.WRITE)
    Long hDel(@ShardKey byte[] key, byte[]... field);

    @Process(Policy.READ)
    Long hLen(@ShardKey byte[] key);

    @Process(Policy.READ)
    Set<byte[]> hKeys(@ShardKey byte[] key);

    @Process(Policy.READ)
    List<byte[]> hVals(@ShardKey byte[] key);

    @Process(Policy.READ)
    Map<byte[], byte[]> hGetAll(@ShardKey byte[] key);

}
