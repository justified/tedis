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
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.taobao.common.tedis.binary.DataType;
import com.taobao.common.tedis.serializer.TedisSerializer;
import com.taobao.common.tedis.util.SortParams;

/**
 * 基本操作入口，包括：
 * <ul>
 *  <li>基本的基于key的操作</li>
 *  <li>取得其他各数据结构操作入口</li>
 *  <li>排序</li>
 *  <li>取得当前序列化方式</li>
 * </ul>
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 上午11:54:51
 * @version 1.0
 */
public interface TedisManager {

    /**
     * <tt>key</tt>是否存在
     * @param namespace
     * @param key
     * @return
     */
    <K> Boolean hasKey(int namespace, K key);

    /**
     * 删除指定<tt>key</tt>
     * @param namespace
     * @param key
     */
    <K> void delete(int namespace, K key);

    /**
     * 批量删除<tt>key</tt>
     * @param namespace
     * @param keys
     */
    <K> void delete(int namespace, Collection<K> keys);

    /**
     * 取得<tt>key</tt>的类型
     * @param namespace
     * @param key
     * @return
     */
    <K> DataType type(int namespace, K key);

    /**
     * 取得<tt>pattern</tt>匹配的<tt>key</tt>集合
     * @param namespace
     * @param pattern
     * @return
     */
    <K> Set<K> keys(int namespace, String pattern);

    /**
     * 重命名<tt>key</tt>
     * @param namespace
     * @param oldKey
     * @param newKey
     */
    <K> void rename(int namespace, K oldKey, K newKey);

    /**
     * 如果<tt>oldKey</tt>存在则重命名<tt>oldKey</tt>为<tt>newKey</tt>
     * @param namespace
     * @param oldKey
     * @param newKey
     * @return
     */
    <K> Boolean renameIfAbsent(int namespace, K oldKey, K newKey);

    /**
     * 设置<tt>key</tt>的过期时间
     * @param namespace
     * @param key
     * @param timeout
     * @param unit
     * @return
     */
    <K> Boolean expire(int namespace, K key, long timeout, TimeUnit unit);

    /**设置<tt>key</tt>的过期时间
     * @param namespace
     * @param key
     * @param date
     * @return
     */
    <K> Boolean expireAt(int namespace, K key, Date date);

    /**
     * 取消<tt>key</tt>的过期设置
     * @param namespace
     * @param key
     * @return
     */
    <K> Boolean persist(int namespace, K key);

    /**
     * 取得<tt>key</tt>的过期时间
     * @param namespace
     * @param key
     * @return
     */
    <K> Long getExpire(int namespace, K key);

    /**
     * 取得计数操作入口
     * @see AtomicCommands
     * @return
     */
    AtomicCommands getAtomicCommands();

    /**
     * 取得String操作入口
     * @see StringCommands
     * @return
     */
    StringCommands getStringCommands();

    /**
     * 取得一般key value操作入口
     * @see ValueCommands
     * @return
     */
    ValueCommands getValueCommands();

    /**
     * 取得Hash操作入口
     * @see HashCommands
     * @param <HK>
     * @param <HV>
     * @return
     */
    HashCommands getHashCommands();

    /**
     * 取得List操作入口
     * @see ListCommands
     * @return
     */
    ListCommands getListCommands();

    /**
     * 取得Set操作入口
     * @see SetCommands
     * @return
     */
    SetCommands getSetCommands();

    /**
     * 取得排序Set操作入口
     * @see ZSetCommands
     * @return
     */
    ZSetCommands getZSetCommands();

    /**
     * 排序
     * @param namespace
     * @param query
     * @return
     */
    <K, V> List<V> sort(int namespace, K key, SortParams params);

    /**
     * 排序
     * @param namespace
     * @param query
     * @param storeKey
     * @return
     */
    <K> Long sort(int namespace, K key, SortParams query, K storeKey);

    /**
     * 取得<tt>value</tt>序列化对象
     * @return
     */
    TedisSerializer<?> getValueSerializer();

    /**
     * 取得<tt>key</tt>序列化对象
     * @return
     */
    TedisSerializer<?> getKeySerializer();

    /**
     * 清理资源，停止所有线程池
     */
    void destroy();

}
