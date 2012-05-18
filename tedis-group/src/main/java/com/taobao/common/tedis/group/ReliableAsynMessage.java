/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.group;

import java.lang.reflect.Method;

import com.juhuasuan.osprey.Message;

/**
 * 可靠异步消息，封装redis命令请求
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-26 上午10:38:58
 * @version 1.0
 */
public class ReliableAsynMessage extends Message {

    private static final long serialVersionUID = 2940060859138293509L;

    private String singleKey;

    private transient Method method;
    private final String clazzName;
    private final String methodName;
    private final String[] types;

    private Object[] args;

    public ReliableAsynMessage(String singleKey, Object[] args, String clazzName, String methodName, String... types) {
        this.singleKey = singleKey;
        this.args = args;
        this.clazzName = clazzName;
        this.methodName = methodName;
        this.types = types;
    }

    public String getSingleKey() {
        return singleKey;
    }

    public void setSingleKey(String singleKey) {
        this.singleKey = singleKey;
    }

    public Method getMethod() throws Exception {
        if (method == null) {
            Class<?>[] classes = new Class<?>[types.length];
            for(int i = 0; i < types.length; i ++) {
                classes[i] = Class.forName(types[i]);
            }
            method = Class.forName(clazzName).getMethod(methodName, classes);
        }
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
    }

    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }

}
