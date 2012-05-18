/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.cache;

import java.util.Collection;
import java.util.Date;
import java.util.Set;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-8-12 上午11:42:11
 * @version 1.0
 */
public interface LocalCache<K, V> {
    /**
     * 保存数据
     * @param key
     * @param value
     * @return
     */
    public V put(K key,V value);

    /**
     * 保存有有效期的数据
     * @param key
     * @param value
     * @param 有效期
     * @return
     */
    public V put(K key,V value, Date expiry);

    /**
     * 保存有有效期的数据
     * @param key
     * @param value
     * @param 数据超时的秒数
     * @return
     */
    public V put(K key,V value, int TTL);

    /**
     * 获取缓存数据
     * @param key
     * @return
     */
    public V get(K key);

    /**
     * 移出缓存数据
     * @param key
     * @return
     */
    public V remove(K key);

    /**
     * 删除所有缓存内的数据
     * @return
     */
    public boolean clear();

    /**
     * 缓存数据数量
     * @return
     */
    public int size();

    /**
     * 缓存所有的key的集合
     * @return
     */
    public Set<K> keySet();

    /**
     * 缓存的所有value的集合
     * @return
     */
    public Collection<V> values();

    /**
     * 是否包含了指定key的数据
     * @param key
     * @return
     */
    public boolean containsKey(K key);

    /**
     * 释放Cache占用的资源
     */
    public void destroy();
}
