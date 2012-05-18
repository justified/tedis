/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator;

import java.io.File;

import com.taobao.common.tedis.replicator.redis.RedisHandler;

import groovy.lang.GroovyClassLoader;

public class GroovyTest {
    public static void main(String[] args) {
        ClassLoader cl = GroovyTest.class.getClassLoader();
        GroovyClassLoader groovycl = new GroovyClassLoader(cl);
        try {
            Class groovyClass = groovycl.parseClass(new File("groovy/DummyHandler.groovy"));
            RedisHandler handler = (RedisHandler)groovyClass.newInstance();
            System.out.println(handler);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
