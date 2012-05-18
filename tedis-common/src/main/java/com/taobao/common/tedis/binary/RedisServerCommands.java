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
import java.util.Properties;

import com.taobao.common.tedis.config.Process;
import com.taobao.common.tedis.config.Process.Policy;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 09:10:46
 * @version 1.0
 */
public interface RedisServerCommands {

    @Process(Policy.WRITE)
    Boolean bgWriteAof();

    @Process(Policy.WRITE)
    Boolean bgSave();

    @Process(Policy.READ)
    Long lastSave();

    @Process(Policy.WRITE)
    Boolean save();

    @Process(Policy.READ)
    Long dbSize();

    @Process(Policy.WRITE)
    Boolean flushDb();

    @Process(Policy.WRITE)
    Boolean flushAll();

    @Process(Policy.READ)
    Properties info();

    @Process(Policy.WRITE)
    Boolean shutdown();

    @Process(Policy.READ)
    List<String> getConfig(String pattern);

    @Process(Policy.WRITE)
    Boolean setConfig(String param, String value);

    @Process(Policy.WRITE)
    Boolean resetConfigStats();

    @Process(Policy.WRITE)
    Long getDB();
}
