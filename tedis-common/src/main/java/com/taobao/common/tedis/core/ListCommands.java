/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.core;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * List操作,更多的时候你可以当场一个队列来操作
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 上午11:54:33
 * @version 1.0
 */
public interface ListCommands {

    /**
     * 取得List中从<tt>start</tt>到<tt>end</tt>的数据<br />
     * 包括<tt>start</tt>和<tt>end</tt>本身指向的数据<br />
     * 例如：<code>range(1,somekey, 0, 10)</code>取得的是11个数据<br />
     * <tt>start</tt>和<tt>end</tt>可为负，表示队列倒数的计数<br />
     * 例如：<code>range(1,somekey, -2, -1)</code>取得的是倒数2个数据
     * @param namespace
     * @param key
     * @param start
     * @param end
     * @return
     */
    <K, V> List<V> range(int namespace, K key, long start, long end);

    /**
     * 截断List，保留从<tt>start</tt>开始到<tt>end</tt>的数据
     * @param namespace
     * @param key
     * @param start
     * @param end
     */
    <K, V> void trim(int namespace, K key, long start, long end);

    /**
     * 取得List大小
     * @param namespace
     * @param key
     * @return
     */
    <K, V> Long size(int namespace, K key);

    /**
     * 从队列头部插入一个数据
     * @param namespace
     * @param key
     * @param value
     * @return
     */
    <K, V> Long leftPush(int namespace, K key, V... value);

    /**
     * 从对ie头部插入一个数据，仅当该<tt>key</tt>不存在才插入数据
     * @param namespace
     * @param key
     * @param value
     * @return
     */
    <K, V> Long leftPushIfPresent(int namespace, K key, V value);

    /**
     * 在<tt>pivot</tt>之前插入一个数据
     * @param namespace
     * @param key
     * @param pivot
     * @param value
     * @return
     */
    <K, V> Long leftInsert(int namespace, K key, V pivot, V value);

    /**
     * 在队列尾插入一个数据
     * @param namespace
     * @param key
     * @param value
     * @return
     */
    <K, V> Long rightPush(int namespace, K key, V... value);

    /**
     * 在队列尾插入一个数据，仅当<tt>key</tt>不存在才插入数据
     * @param namespace
     * @param key
     * @param value
     * @return
     */
    <K, V> Long rightPushIfPresent(int namespace, K key, V value);

    /**
     * 在<tt>pivot</tt>之后插入一个数据
     * @param namespace
     * @param key
     * @param pivot
     * @param value
     * @return
     */
    <K, V> Long rightInsert(int namespace, K key, V pivot, V value);

    /**
     * 更新位置<tt>index</tt>的值
     * @param namespace
     * @param key
     * @param index
     * @param value
     */
    <K, V> void set(int namespace, K key, long index, V value);

    /**
     * 删除List中<tt>i</tt>值为<tt>value</tt>数据
     * <ul>
     *  <li>i大于0：从队列头开始删除i个数据</li>
     *  <li>i小于0：从队列尾开始删除i个数据</li>
     *  <li>i等于0：删除所有值为<tt>value</tt>的数据</li>
     * </ul>
     * @param namespace
     * @param key
     * @param i
     * @param value
     * @return
     */
    <K, V> Long remove(int namespace, K key, long i, Object value);

    /**
     * 取得位于<tt>index</tt>处的数据
     * @param namespace
     * @param key
     * @param index
     * @return
     */
    <K, V> V index(int namespace, K key, long index);

    /**
     * 删除并取出队列头部的一个数据，如果不存在立即返回null
     * @param namespace
     * @param key
     * @return
     */
    <K, V> V leftPop(int namespace, K key);

    /**
     * 删除并取出队列头部的一个数据，如果不存在则等待指定的超时时间<br />
     * <tt>timeout</tt>为0则表示永不超时
     * @param namespace
     * @param key
     * @param timeout
     * @param unit
     * @return
     */
    <K, V> V leftPop(int namespace, K key, long timeout, TimeUnit unit);

    /**
     * 删除并取出队列尾部的一个数据，如果不存在则立即返回null
     * @param namespace
     * @param key
     * @return
     */
    <K, V> V rightPop(int namespace, K key);

    /**
     * 删除并取出队列尾部的一个数据，如果不存在则等待指定的超时时间<br />
     * <tt>timeout</tt>为0则表示永不超时
     * @param namespace
     * @param key
     * @param timeout
     * @param unit
     * @return
     */
    <K, V> V rightPop(int namespace, K key, long timeout, TimeUnit unit);

    /**
     * 将<tt>sourceKey</tt>的尾部的一个数据删除并插入到<tt>destinationKey</tt>的头部，并且将该数据返回，如果数据不存在则立即返回null
     * @param namespace
     * @param sourceKey
     * @param destinationKey
     * @return
     */
    <K, V> V rightPopAndLeftPush(int namespace, K sourceKey, K destinationKey);

    /**
     * 将<tt>sourceKey</tt>的尾部的一个数据删除并插入到<tt>destinationKey</tt>的头部，并且将该数据返回，如果数据不存在则等待指定时间
     * <tt>timeout</tt>为0则表示永不超时
     * @param namespace
     * @param sourceKey
     * @param destinationKey
     * @param timeout
     * @param unit
     * @return
     */
    <K, V> V rightPopAndLeftPush(int namespace, K sourceKey, K destinationKey, long timeout, TimeUnit unit);

}
