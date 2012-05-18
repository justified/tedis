/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.binary;

import com.taobao.common.tedis.config.Process;
import com.taobao.common.tedis.config.Process.Policy;

/**
 * 
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 09:06:05
 * @version 1.0
 */
public interface RedisConnectionCommands {

    @Process(Policy.WRITE)
    Boolean select(int dbIndex);

    @Process(Policy.WRITE)
    byte[] echo(byte[] message);

    @Process(Policy.WRITE)
    Boolean ping();

    @Process(Policy.WRITE)
    Boolean connect();

    @Process(Policy.WRITE)
    Boolean disconnect();

    @Process(Policy.WRITE)
    Boolean quit();

    @Process(Policy.WRITE)
    Boolean auth(String password);

}
