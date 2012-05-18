/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.taobao.common.tedis.group;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.juhuasuan.osprey.OspreyManager;
import com.juhuasuan.osprey.OspreyProcessor;
import com.juhuasuan.osprey.Result;
import com.taobao.common.tedis.Group;
import com.taobao.common.tedis.Single;
import com.taobao.common.tedis.TedisConnectionException;
import com.taobao.common.tedis.TedisException;
import com.taobao.common.tedis.binary.RedisCommands;
import com.taobao.common.tedis.config.ConfigManager;
import com.taobao.common.tedis.config.Process;
import com.taobao.common.tedis.config.Process.Policy;
import com.taobao.common.tedis.config.Router;
import com.taobao.common.tedis.monitor.BufferedStatLogWriter;

/**
 * 可靠异步的方式执行命令, 屏蔽下层访问细节
 *
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-26 上午10:37:19
 * @version 1.0
 */
public class ReliableAsynTedisGroup implements Group {

    static final Log logger = LogFactory.getLog(ReliableAsynTedisGroup.class);

    private ConfigManager cm;
    private RedisCommands tedis;
    private String appName;
    private String version;
    private volatile boolean inited = false;

    private OspreyManager ospreyManager;

    public ReliableAsynTedisGroup(String appName, String version) {
        this.appName = appName;
        this.version = version;
    }

    public void init() {
        if (!inited) {
            try {
                if (this.cm == null) {
                    this.cm = new DiamondConfigManager(appName, version);
                }
                this.ospreyManager = new OspreyManager("tedis-" + appName + "-" + version);
                this.ospreyManager.registerProcessor(new OspreyProcessor<ReliableAsynMessage>() {

                    @Override
                    public Class<ReliableAsynMessage> interest() {
                        return ReliableAsynMessage.class;
                    }

                    @Override
                    public Result process(ReliableAsynMessage message) {
                        Result result = new Result();
                        long time = System.currentTimeMillis();

                        Single single = cm.getRouter().getAtomic(message.getSingleKey());
                        if (single == null) {
                            result.setSuccess(false);
                            result.setErrorMessage("Current atomic is null");
                            return result;
                        }

                        try {
                            message.getMethod().invoke(single.getTedis(), message.getArgs());
                        } catch (Throwable t) {
                            logger.warn("write exception:" + single.getProperties(), t);
                            try {
                                statLog(message.getMethod().getName(), false, time);
                                // 解包异常
                                InvocationTargetException ite = (InvocationTargetException) t;
                                UndeclaredThrowableException ute = (UndeclaredThrowableException) ite.getTargetException();
                                if (ute.getUndeclaredThrowable() instanceof TimeoutException) {
                                    result.setSuccess(false);
                                    result.setErrorMessage("TimeoutException");
                                    result.setRuntimeException(ute);
                                    return result;
                                } else {
                                    ExecutionException ee = (ExecutionException) ute.getUndeclaredThrowable();
                                    InvocationTargetException ite_1 = (InvocationTargetException) ee.getCause();
                                    TedisException te = (TedisException) ite_1.getTargetException();
                                    if (te.getCause() instanceof TedisConnectionException) {
                                        result.setSuccess(false);
                                        result.setErrorMessage("JedisConnectionException");
                                        result.setRuntimeException(te);
                                        return result;
                                    }
                                }
                            } catch (Throwable tt) {
                                logger.warn("解包异常:", tt);
                            }
                        }
                        return result;
                    }
                });
                this.ospreyManager.init();
                tedis = (RedisCommands) Proxy.newProxyInstance(RedisCommands.class.getClassLoader(), new Class[] { RedisCommands.class }, new TedisGroupInvocationHandler());
            } catch (Exception e) {
                throw new TedisException("init failed", e);
            }
            inited = true;
        }
    }

    public RedisCommands getTedis() {
        if (tedis == null) {
            throw new TedisException("please invoke the init method first.");
        }
        return tedis;
    }

    public class TedisGroupInvocationHandler implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            long time = System.currentTimeMillis();
            String name = method.getName();
            Router rr = cm.getRouter();
            Process annotation = method.getAnnotation(Process.class);
            Throwable exception = null;
            if (annotation.value() == Policy.READ) {
                while (rr.getRouteData().props.size() > 0) {
                    Single s = rr.route();
                    try {
                        Object result = method.invoke(s.getTedis(), args);
                        statLog(name, true, time);
                        return result;
                    } catch (Throwable t) {
                        statLog(name, false, time);
                        exception = t;
                        logger.warn("read exception:" + s.getProperties(), t);
                        boolean connectionError = false;
                        try {
                            if (t instanceof InvocationTargetException) {// 解包异常
                                InvocationTargetException ite = (InvocationTargetException) t;
                                UndeclaredThrowableException ute = (UndeclaredThrowableException) ite.getTargetException();
                                if (ute.getUndeclaredThrowable() instanceof TimeoutException) {
                                    connectionError = true;
                                    rr.onError(s);
                                } else {
                                    ExecutionException ee = (ExecutionException) ute.getUndeclaredThrowable();
                                    InvocationTargetException ite_1 = (InvocationTargetException) ee.getCause();
                                    TedisException te = (TedisException) ite_1.getTargetException();
                                    if (te.getCause() instanceof TedisConnectionException) {
                                        connectionError = true;
                                        rr.onError(s);
                                    }
                                }
                            }
                        } catch (Throwable tt) {
                            logger.warn("解包异常:", tt);
                            // 可能会抛出转换异常,符合预期,如果碰到转换异常,直接在connection error
                            // 过程中从新抛出
                        }

                        if (!connectionError) {
                            throw t;
                        }
                    }
                }

                throw new Exception("read RouteData is empty," + rr, exception);
            } else if (annotation.value() == Policy.WRITE) {
                Single[] ss = rr.getAllRouteData().group;
                if (ss == null || ss.length == 0) {
                    throw new Exception("write RouteData is empty," + rr, exception);
                }
                Object result = null;
                int e = 0;
                List<ReliableAsynMessage> messages = null;
                for (Single s : ss) {
                    try {
                        result = method.invoke(s.getTedis(), args);
                    } catch (Throwable t) {
                        e++;
                        statLog(name, false, time);
                        logger.warn("write exception:" + s.getProperties(), t);
                        exception = t;
                        try {
                            // 解包异常
                            InvocationTargetException ite = (InvocationTargetException) t;
                            UndeclaredThrowableException ute = (UndeclaredThrowableException) ite.getTargetException();
                            if (ute.getUndeclaredThrowable() instanceof TimeoutException) {
                                messages = onError(args, name, rr, messages, s);
                            } else {
                                ExecutionException ee = (ExecutionException) ute.getUndeclaredThrowable();
                                InvocationTargetException ite_1 = (InvocationTargetException) ee.getCause();
                                TedisException te = (TedisException) ite_1.getTargetException();
                                if (te.getCause() instanceof TedisConnectionException) {
                                    messages = onError(args, name, rr, messages, s);
                                }
                            }
                        } catch (Throwable tt) {
                            logger.warn("解包异常:", tt);
                        }
                    }
                }

                if (e >= 2) {// 全部都抛异常了,告知调用端
                    throw exception;
                }

                if (messages != null && messages.size() > 0) {
                    for (ReliableAsynMessage m : messages) {
                        ospreyManager.addMessage(m, true);
                    }
                    messages.clear();
                }

                statLog(name, true, time);
                return result;
            } else if ("toString".equals(name)) {
                return "";
            } else if ("hashCode".equals(name)) {
                Single s = rr.route();
                if (s != null) {
                    return s.hashCode();
                } else {
                    return 0;
                }
            } else if ("equals".equals(name)) {
                Single s = rr.route();
                if (args.length == 1) {
                    return s.equals(args[0]);
                }
            }
            statLog(name, false, time);
            throw new Exception("method don't match:" + name);
        }

        private List<ReliableAsynMessage> onError(Object[] args, String name, Router rr, List<ReliableAsynMessage> messages, Single s) {
            if (rr.getAtomic(s.getProperties().generateKey()) != null) {
                rr.onError(s);
            }
            if (messages == null) {
                messages = new ArrayList<ReliableAsynMessage>();
            }
            String[] types = new String[args.length];
            for(int n = 0; n< args.length ; n++) {
                types[n] = args[n].getClass().getName();
            }
            messages.add(new ReliableAsynMessage(s.getProperties().generateKey(), args, RedisCommands.class.getName(), name, types));
            return messages;
        }
    }

    private void statLog(String methodName, Boolean flag, long time) {
        BufferedStatLogWriter.add(appName, methodName, flag, 1, System.currentTimeMillis() - time);
    }

    public void destroy() {
        Router rr = cm.getRouter();
        rr.destroy();
        // ConfigMananger.destroy();
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public ConfigManager getConfigManager() {
        return this.cm;
    }

    @Override
    public void setConfigManager(ConfigManager cm) {
        this.cm = cm;
    }
}
