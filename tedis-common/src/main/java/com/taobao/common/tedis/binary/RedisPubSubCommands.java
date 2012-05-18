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
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 09:36:07
 * @version 1.0
 */
public interface RedisPubSubCommands {

    @Process(Policy.READ)
    boolean isSubscribed();

    @Process(Policy.WRITE)
    Long publish(byte[] channel, byte[] message);

    @Process(Policy.WRITE)
    Boolean subscribe(MessageListener listener, byte[]... channels);

    @Process(Policy.WRITE)
    Boolean pSubscribe(MessageListener listener, byte[]... patterns);
}
