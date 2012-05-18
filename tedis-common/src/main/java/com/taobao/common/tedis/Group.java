/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis;

import com.taobao.common.tedis.binary.RedisCommands;
import com.taobao.common.tedis.config.ConfigManager;

/**
 * 集群代理，屏蔽下层访问细节
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-4-23 上午10:20:11
 * @version 1.0
 */
public interface Group {

    /**
     * 获取集群操作入口
     * @return
     */
    RedisCommands getTedis();

    /**
     * 注销集群实例
     */
    void destroy();

    /**
     * 设置配置管理器，默认实现有：Diamond，ZK和文件
     * @param cm
     */
    void setConfigManager(ConfigManager cm);

    /**
     * 使用集群之前需要先初始化
     */
    void init();

}
