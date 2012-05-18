/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.core;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * String操作
 * 
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-8-4 下午09:07:01
 * @version 1.0
 */
public interface StringCommands {

    /**
     * 在为<tt>key</tt>的String追加<tt>value</tt>组成新的字符串，并返回组装后的字符串长度
     * 
     * @param namespace
     * @param key
     * @param value
     * @return
     */
    Long append(int namespace, String key, String value);

    /**
     * 得到String从位置<tt>start</tt>到<tt>end</tt>的子字符串
     * 
     * @param namespace
     * @param key
     * @param start
     * @param end
     * @return
     */
    String get(int namespace, String key, long start, long end);

    /**
     * 从<tt>offset</tt>开始替换为<tt>value</tt>
     * 
     * @param namespace
     * @param key
     * @param value
     * @param offset
     */
    void set(int namespace, String key, String value, long offset);

    /**
     * 取得字符串长度
     * 
     * @param namespace
     * @param key
     * @return
     */
    Long size(int namespace, String key);

    /**
     * 设置数据，如果数据已经存在，则覆盖，如果不存在，则新增
     * 
     * @param namespace
     *            数据所在的namespace
     * @param key
     * @param value
     */
    void set(int namespace, String key, String value);

    /**
     * 设置数据，如果数据已经存在，则覆盖，如果不存在，则新增
     * 
     * @param namespace
     *            数据所在的namespace
     * @param key
     * @param value
     * @param timeout
     *            数据的有效时间
     * @param unit
     *            时间单位
     */
    void set(int namespace, String key, String value, long timeout, TimeUnit unit);

    /**
     * 设置数据，如果数据已经存在，则保留原值返回<tt>false</tt>，如果不存在，则新增，返回<tt>true</tt>
     * 
     * @param namespace
     *            数据所在的namespace
     * @param key
     * @param value
     * @return 如果数据不存在返回<tt>true</tt>，否则返回<tt>false</tt>
     */
    Boolean setIfAbsent(int namespace, String key, String value);

    /**
     * 批量设置数据
     * 
     * @param namespace
     * @param m
     */
    void multiSet(int namespace, Map<String, String> m);

    /**
     * 批量设置数据，只有当数据不存在时才设置
     * 
     * @param namespace
     * @param m
     */
    void multiSetIfAbsent(int namespace, Map<String, String> m);

    /**
     * 获取数据 注意不能用此方法去获取increment方法产生的key
     * 
     * @param namespace
     * @param key
     * @return
     */
    String get(int namespace, Object key);

    /**
     * 设置数据同时返回老数据，此方法是原子操作 注意不能用此方法去获取increment方法产生的key
     * 
     * @param namespace
     * @param key
     * @param value
     * @return
     */
    String getAndSet(int namespace, String key, String value);

    /**
     * 批量获取数据 注意不能用此方法去获取increment方法产生的key
     * 
     * @param namespace
     * @param keys
     * @return
     */
    List<String> multiGet(int namespace, Collection<String> keys);

}
