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

import com.taobao.common.tedis.config.Process;
import com.taobao.common.tedis.config.Process.Policy;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 09:37:55
 * @version 1.0
 */
public interface RedisTransactionCommands {

    @Process(Policy.WRITE)
    Boolean multi();

    @Process(Policy.WRITE)
    List<Object> exec();

    @Process(Policy.WRITE)
    Boolean discard();

    @Process(Policy.WRITE)
    Boolean watch(byte[]... keys);

    @Process(Policy.WRITE)
    Boolean unwatch();
}
