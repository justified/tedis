/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.binary;

import java.util.Set;

import com.taobao.common.tedis.config.Process;
import com.taobao.common.tedis.config.ShardKey;
import com.taobao.common.tedis.config.Process.Policy;
import com.taobao.common.tedis.config.ShardKey.Type;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 09:11:26
 * @version 1.0
 */
public interface RedisSetCommands {

    @Process(Policy.WRITE)
    Long sAdd(@ShardKey byte[] key, byte[]... value);

    @Process(Policy.WRITE)
    Long sRem(@ShardKey byte[] key, byte[]... value);

    @Process(Policy.READ)
    byte[] sPop(@ShardKey byte[] key);

    @Process(Policy.WRITE)
    Boolean sMove(@ShardKey byte[] srcKey, byte[] destKey, byte[] value);

    @Process(Policy.READ)
    Long sCard(@ShardKey byte[] key);

    @Process(Policy.READ)
    Boolean sIsMember(@ShardKey byte[] key, byte[] value);

    @Process(Policy.READ)
    Set<byte[]> sInter(@ShardKey(Type.MULTI) byte[]... keys);

    @Process(Policy.WRITE)
    Long sInterStore(@ShardKey byte[] destKey, byte[]... keys);

    @Process(Policy.READ)
    Set<byte[]> sUnion(@ShardKey(Type.MULTI) byte[]... keys);

    @Process(Policy.WRITE)
    Long sUnionStore(@ShardKey byte[] destKey, byte[]... keys);

    @Process(Policy.READ)
    Set<byte[]> sDiff(@ShardKey(Type.MULTI) byte[]... keys);

    @Process(Policy.WRITE)
    Long sDiffStore(@ShardKey byte[] destKey, byte[]... keys);

    @Process(Policy.READ)
    Set<byte[]> sMembers(@ShardKey byte[] key);

    @Process(Policy.READ)
    byte[] sRandMember(@ShardKey byte[] key);
}
