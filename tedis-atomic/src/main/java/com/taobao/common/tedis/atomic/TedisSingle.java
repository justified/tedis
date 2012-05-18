/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.atomic;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.common.tedis.Single;
import com.taobao.common.tedis.TedisException;
import com.taobao.common.tedis.binary.RedisCommands;
import com.taobao.common.tedis.config.HAConfig.ServerProperties;
import com.taobao.common.tedis.config.ShardKey;
import com.taobao.common.tedis.config.ShardKey.Type;
import com.taobao.common.tedis.core.BaseCommands;
import com.taobao.common.tedis.serializer.SerializationUtils;

/**
 * @author jianxing <jianxing.qx@taobao.com>
 */
@SuppressWarnings("unchecked")
public class TedisSingle implements Single {

    static final Log logger = LogFactory.getLog(TedisSingle.class);
    // private TedisPool pool;
    private AtomicInteger errorCount = new AtomicInteger(0);
    private ServerProperties prop;
    RedisCommands tedis;
    int pool_size;
    // int max_batch_size = 100;
    int requestQueueLimit = 10000;
    List<BatchThread> threadPool = new ArrayList<BatchThread>();
    Class pipeline;
    final ArrayBlockingQueue<Request> requestQueue = new ArrayBlockingQueue(requestQueueLimit);

    @Override
    public ServerProperties getProperties() {
        return this.prop;
    }

    @Override
    public AtomicInteger getErrorCount() {
        return this.errorCount;
    }

    public TedisSingle(ServerProperties prop) {
        this.prop = prop;

        this.pool_size = prop.pool_size;
        this.tedis = (RedisCommands) Proxy.newProxyInstance(RedisCommands.class.getClassLoader(), new Class[]{RedisCommands.class}, new TedisInvocationHandler());

        for (int i = 0; i < pool_size; i++) {
            Tedis[] _tedises = new Tedis[prop.servers.length];
            for (int j = 0; j < prop.servers.length; j++) {
                Tedis _tedis = new Tedis(prop.servers[j].addr, prop.servers[j].port, prop.timeout);
                if (null != prop.password && !"".equals(prop.password)) {
                    _tedis.auth(prop.password);
                } else {
                    _tedis.ping();
                }
                _tedises[j] = _tedis;
            }

            BatchThread thread = new BatchThread(i, _tedises);
            threadPool.add(thread);
            thread.start();
        }
    }

    @Override
    public RedisCommands getTedis() {
        return tedis;
    }

    public class Request {

        Method method;
        Object[] args;
        BatchFuture result;
    }

    private class TedisInvocationHandler implements InvocationHandler {

        public Object batch(Object proxy, Method method, Object[] args) throws Throwable {
            Request req = new Request();
            req.method = method;
            req.args = args;
            req.result = new BatchFuture();

            try {
                requestQueue.add(req);
            } catch (Throwable t) {
                req.result.setException(t);
            }
            if (logger.isDebugEnabled()) {
                logger.debug(prop + ",method:" + method.getName());
            }
            Object result = req.result.get(prop.timeout, TimeUnit.MILLISECONDS);
            if (logger.isDebugEnabled()) {
                logger.debug("result:" + (result == null ? "ok" : result.toString()));
            }
            return result;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

            return batch(proxy, method, args);
        }
    }

    private class BatchThread extends Thread {

        Tedis[] tedis;
        int i;
        long[] timeMap;
        volatile boolean stop;

        public BatchThread(int i, Tedis[] tedis) {
            this.i = i;
            this.tedis = tedis;
            this.timeMap = new long[tedis.length];
        }

        @Override
        public void run() {
            Thread.currentThread().setName("BatchThread-" + i);
            while (!stop) {
                Request r = null;
                try {
                    while (!stop && r == null) {
                        r = requestQueue.poll(200, TimeUnit.SECONDS);
                        if (r == null) {
                            for (Tedis t : tedis) {
                                t.ping();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    logger.warn(e.getMessage());
                    break;
                } catch (Exception e) {
                    logger.error("Request poll error:", e);
                }
                if (r != null) {
                    try {
                        List<Object> rets = new ArrayList<Object>();
                        for (Tedis t : getShardedTedis(r)) {
                            rets.add(r.method.invoke(t, r.args));
                        }
                        r.result.setResult(handleRets(type(r), rets));
                    } catch (Throwable t) {
                        r.result.setException(t);
                    }
                }

            }
            for (Tedis t : tedis) {
                try {
                    t.disconnect();
                } catch (Exception e1) {
                    logger.warn("断开连接失败", e1);
                }
            }
        }

        public void stop1(){
            stop = true;
        }

        private Object handleRets(Type type, List<Object> rets) {
            switch (type) {
                case RT_OBOJECT:
                    if (rets.size() > 0) {
                        return rets.get(0);
                    }
                    return null;
                case RT_SET:
                    Set<byte[]> result = null;
                    for (Object r : rets) {
                        if (result == null) {
                            result = (Set<byte[]>) r;
                        } else {
                            result.addAll((Set<byte[]>) r);
                        }
                    }
                    return result;
                default:
                    throw new TedisException("Error: missing shard return type");
            }
        }

        private Tedis[] getShardedTedis(Request request) {
            int hash = (int) hash(request) % tedis.length;
            if (hash < 0) {
                return tedis;
            }
            long now = System.currentTimeMillis();
            timeMap[hash] = now;
            for (int i = 0; i < tedis.length; i++) {
                if (i != hash && (now - timeMap[i]) > 200 * 1000) {
                    tedis[i].ping();
                    timeMap[i] = now;
                }
            }
            return new Tedis[]{tedis[hash]};
        }

        private Type type(Request request) {
            Annotation[][] as = request.method.getParameterAnnotations();
            for (int i = 0; i < as.length; i++) {
                if (as[i].length > 0 && as[i][0] instanceof ShardKey) {
                    return ((ShardKey) as[i][0]).retType();
                }
            }
            return Type.RT_OBOJECT;
        }

        private long hash(Request request) {
            Annotation[][] as = request.method.getParameterAnnotations();
            for (int i = 0; i < as.length; i++) {
                if (as[i].length > 0 && as[i][0] instanceof ShardKey) {
                    Type type = ((ShardKey) as[i][0]).value();
                    return Long.parseLong(SerializationUtils.deserialize(getRouteFromKey(request.args[i], type)));
                }
            }
            return -1;
        }

        private byte[] getRouteFromKey(Object key, Type type) {
            switch (type) {
                case SINGLE:
                    return getSingle((byte[]) key);
                case MULTI:
                    byte[][] multikey = (byte[][]) key;
                    return getSingle(multikey[0]);
                case MAP:
                    Map<byte[], byte[]> mapkey = (Map<byte[], byte[]>) key;
                    return getSingle(mapkey.keySet().iterator().next());
                default:
                    throw new TedisException("Error: missing shard type");
            }
        }

        private byte[] getSingle(byte[] key) {
            int i = 0;
            while (i < key.length && key[i] != BaseCommands.PART[0]) {
                i++;
            }
            if (i >= key.length) {
                return new byte[]{'-', '1'};
            }
            byte[] result = new byte[i];
            System.arraycopy(key, 0, result, 0, i);
            return result;
        }
    }

    public void destroy() {
        for (BatchThread thread : threadPool) {
            thread.stop1();
            thread.interrupt();
        }
        //threadPool.clear();

        Request r = null;
        while ((r = requestQueue.poll()) != null) {
            r.result.setException(new Exception("Single实例不可用,销毁请求队列"));
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TedisSingle other = (TedisSingle) obj;
        if (this.prop != other.prop && (this.prop == null || !this.prop.equals(other.prop))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        return hash;
    }

    @Override
    public String toString() {
        return "Single{" + "errorCount=" + errorCount + ", prop=" + prop + '}';
    }

    private class BatchFuture<T> implements Future<T> {

        CountDownLatch cdl = new CountDownLatch(1);
        volatile T result;
        volatile Throwable t;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isCancelled() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isDone() {
            return cdl.getCount() <= 0;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            boolean to = cdl.await(timeout, unit);
            if (!to) {
                throw new TimeoutException("future cdl timeout");
            }
            if (t != null) {
                throw new ExecutionException(t);
            }
            return result;
        }

        public void setResult(T result) {
            this.result = result;
            cdl.countDown();
        }

        public void setException(Throwable t) {
            this.t = t;
            cdl.countDown();
        }
    }
}
