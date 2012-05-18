/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.cache;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-8-12 11:43:52
 * @version 1.0
 */
public class DefaultLocalCache<K, V> implements LocalCache<K, V> {

    private static final Log Logger = LogFactory.getLog(DefaultLocalCache.class);

    ConcurrentHashMap<K, SoftReference<V>>[] caches;
    ConcurrentHashMap<K, Long> expiryCache;

    private ScheduledExecutorService scheduleService;

    private int expiryInterval = 10;

    private int moduleSize = 10;

    public DefaultLocalCache() {
        init();
    }

    public DefaultLocalCache(int expiryInterval, int moduleSize) {
        this.expiryInterval = expiryInterval;
        this.moduleSize = moduleSize;
        init();
    }

    @SuppressWarnings("unchecked")
    private void init() {
        caches = new ConcurrentHashMap[moduleSize];

        for (int i = 0; i < moduleSize; i++) {
            caches[i] = new ConcurrentHashMap<K, SoftReference<V>>();
        }

        expiryCache = new ConcurrentHashMap<K, Long>();

        scheduleService = Executors.newScheduledThreadPool(1);

        scheduleService.scheduleAtFixedRate(new CheckOutOfDateSchedule(caches, expiryCache), 0, expiryInterval * 60, TimeUnit.SECONDS);

        if (Logger.isInfoEnabled()) {
            Logger.info("DefaultCache CheckService is start!");
        }
    }

    public boolean clear() {
        if (caches != null)
            for (ConcurrentHashMap<K, SoftReference<V>> cache : caches) {
                cache.clear();
            }

        if (expiryCache != null) {
            expiryCache.clear();
        }

        return true;
    }

    public boolean containsKey(K key) {
        checkValidate(key);
        return getCache(key).containsKey(key);
    }

    public V get(K key) {
        checkValidate(key);
        SoftReference<V> sr = getCache(key).get(key);
        if (sr == null) {
            expiryCache.remove(key);
            return null;
        } else {
            return sr.get();
        }
    }

    public Set<K> keySet() {
        checkAll();
        return expiryCache.keySet();
    }

    public V put(K key, V value) {
        SoftReference<V> result = getCache(key).put(key, new SoftReference<V>(value));
        expiryCache.put(key, -1L);

        return result == null ? null : result.get();
    }

    public V put(K key, V value, Date expiry) {
        SoftReference<V> result = getCache(key).put(key, new SoftReference<V>(value));
        expiryCache.put(key, expiry.getTime());

        return result == null ? null : result.get();
    }

    public V remove(K key) {
        SoftReference<V> result = getCache(key).remove(key);
        expiryCache.remove(key);
        return result == null ? null : result.get();
    }

    public int size() {
        checkAll();

        return expiryCache.size();
    }

    public Collection<V> values() {
        checkAll();

        Collection<V> values = new ArrayList<V>();

        for (ConcurrentHashMap<K, SoftReference<V>> cache : caches) {
            for (SoftReference<V> sr : cache.values()) {
                values.add(sr.get());
            }
        }

        return values;
    }

    private ConcurrentHashMap<K, SoftReference<V>> getCache(K key) {
        long hashCode = (long) key.hashCode();

        if (hashCode < 0)
            hashCode = -hashCode;

        int moudleNum = (int) hashCode % moduleSize;

        return caches[moudleNum];
    }

    private void checkValidate(K key) {
        if (key != null && expiryCache.get(key) != null && expiryCache.get(key) != -1 && new Date(expiryCache.get(key)).before(new Date())) {
            getCache(key).remove(key);
            expiryCache.remove(key);
        }
    }

    private void checkAll() {
        Iterator<K> iter = expiryCache.keySet().iterator();

        while (iter.hasNext()) {
            K key = iter.next();
            checkValidate(key);
        }
    }

    class CheckOutOfDateSchedule implements Runnable {
        ConcurrentHashMap<K, SoftReference<V>>[] caches;
        ConcurrentHashMap<K, Long> expiryCache;

        public CheckOutOfDateSchedule(ConcurrentHashMap<K, SoftReference<V>>[] caches, ConcurrentHashMap<K, Long> expiryCache) {
            this.caches = caches;
            this.expiryCache = expiryCache;
        }

        public void run() {
            check();
        }

        public void check() {
            try {
                for (ConcurrentHashMap<K, SoftReference<V>> cache : caches) {
                    Iterator<K> keys = cache.keySet().iterator();

                    while (keys.hasNext()) {
                        K key = keys.next();

                        if (expiryCache.get(key) == null)
                            continue;

                        long date = expiryCache.get(key);

                        if ((date > 0) && (new Date(date).before(new Date()))) {
                            expiryCache.remove(key);
                            cache.remove(key);
                        }

                    }

                }
            } catch (Exception ex) {
                Logger.info("DefaultCache CheckService is start!");
            }
        }

    }

    @Override
    public V put(K key, V value, int TTL) {
        SoftReference<V> result = getCache(key).put(key, new SoftReference<V>(value));

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, TTL);
        expiryCache.put(key, calendar.getTime().getTime());

        return result == null ? null : result.get();
    }

    public void destroy() {
        try {
            clear();

            if (scheduleService != null)
                scheduleService.shutdown();

            scheduleService = null;
        } catch (Exception ex) {
            Logger.error(ex);
        }
    }

}
