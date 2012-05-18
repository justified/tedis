/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis;

import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.common.tedis.binary.RedisCommands;
import com.taobao.common.tedis.config.HAConfig.ServerProperties;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-4-23 上午10:20:03
 * @version 1.0
 */
public interface Single {

    /**
     * 获取单个Redis操作入口
     * @return
     */
    RedisCommands getTedis();

    /**
     * 获取单个实例的配置
     * @return
     */
    ServerProperties getProperties();

    /**
     * 获取单个实例的失败次数
     * @return
     */
    AtomicInteger getErrorCount();

}
