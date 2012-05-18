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
import java.util.Map;
import java.util.Set;

/**
 * Hash操作
 *
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 上午11:54:26
 * @version 1.0
 */
public interface HashCommands {

    /**
     * 删除<tt>key</tt>的Hash对象的<tt>hashKey</tt>的数据
     *
     * @param namespace
     * @param key
     * @param hashKey
     */
    <H, HK, HV> void delete(int namespace, H key, Object... hashKey);

    /**
     * 判断<tt>key</tt>的Hash对象的<tt>hashKey</tt>是否存在
     *
     * @param namespace
     * @param key
     * @param hashKey
     * @return 存在返回<tt>true</tt>，不存在返回<tt>false</tt>
     */
    <H, HK, HV> Boolean hasKey(int namespace, H key, Object hashKey);

    /**
     * 取得<tt>key</tt>的Hash对象的<tt>hashKey</tt>的数据
     *
     * @param namespace
     * @param key
     * @param hashKey
     * @return
     */
    <H, HK, HV> HV get(int namespace, H key, Object hashKey);

    /**
     * 批量取得数据
     *
     * @param namespace
     * @param key
     * @param hashKeys
     * @return
     */
    <H, HK, HV> Collection<HV> multiGet(int namespace, H key, Collection<HK> hashKeys);

    /**
     * 计数操作方法 <tt>delta</tt>可为正、负、0值。正值为增加计数、负值为减少计数、0为取得当前值
     *
     * @param namespace
     * @param key
     * @param hashKey
     * @param delta
     * @return
     */
    <H, HK, HV> Long increment(int namespace, H key, HK hashKey, long delta);

    /**
     * 得到<tt>key</tt>的Hash对象的所有key
     *
     * @param namespace
     * @param key
     * @return
     */
    <H, HK, HV> Set<HK> keys(int namespace, H key);

    /**
     * 缺的<tt>key</tt>的Hash对象的大小
     *
     * @param namespace
     * @param key
     * @return
     */
    <H, HK, HV> Long size(int namespace, H key);

    /**
     * 批量设置数据
     *
     * @param namespace
     * @param key
     * @param m
     */
    <H, HK, HV> void putAll(int namespace, H key, Map<? extends HK, ? extends HV> m);

    /**
     * 设置数据，如果数据存在，则覆盖，如果数据不存在，则新增
     *
     * @param namespace
     * @param key
     * @param hashKey
     * @param value
     */
    <H, HK, HV> void put(int namespace, H key, HK hashKey, HV value);

    /**
     * 设置数据，只有数据不存在才能设置成功
     *
     * @param namespace
     * @param key
     * @param hashKey
     * @param value
     * @return
     */
    <H, HK, HV> Boolean putIfAbsent(int namespace, H key, HK hashKey, HV value);

    /**
     * 取得<tt>key</tt>的Hash对象的所有值
     *
     * @param namespace
     * @param key
     * @return
     */
    <H, HK, HV> Collection<HV> values(int namespace, H key);

    /**
     * 缺的<tt>key</tt>的Hash对象的所有key、value
     *
     * @param namespace
     * @param key
     * @return
     */
    <H, HK, HV> Map<HK, HV> entries(int namespace, H key);

}
