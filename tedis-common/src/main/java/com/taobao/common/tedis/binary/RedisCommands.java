/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.binary;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 上午11:46:49
 * @version 1.0
 */
public interface RedisCommands extends RedisKeyCommands, RedisValueCommands, RedisListCommands, RedisSetCommands, RedisZSetCommands, RedisHashCommands, RedisConnectionCommands, RedisServerCommands {

}
