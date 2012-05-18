/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.redis;

import groovy.lang.GroovyClassLoader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.conf.ReplicatorRuntimeConf;
import com.taobao.common.tedis.replicator.conf.ServerRuntimeException;

public class RedisHandlerLoader {
	private static final Logger logger = Logger.getLogger(RedisHandlerLoader.class);
	private static final String GROOVY_SUFFIX = ".groovy";

	private static ClassLoader cl = RedisHandlerLoader.class.getClassLoader();
	private static GroovyClassLoader groovyCl = new GroovyClassLoader(cl);

	private static File redisHandlerPath;

	public RedisHandlerLoader(String serviceName) {
		redisHandlerPath = ReplicatorRuntimeConf.getConfiguration(serviceName).getRedisHandlerDir();
		if (redisHandlerPath == null) {
			throw new ServerRuntimeException("Redis handler path not set.");
		}
	}

	@SuppressWarnings({ "rawtypes" })
	public RedisHandler load(String name) throws RedisHandleException {
		if (name == null || name.isEmpty()) {
			return null;
		}
		name = name.trim();
		String className = name;//name.substring(0, 1).toUpperCase() + name.substring(1);

		try {
			Class clazz = cl.loadClass(className);
			RedisHandler handler = (RedisHandler)clazz.newInstance();
			return handler;
		} catch (Exception e) {
			// ignore
		}

		className += GROOVY_SUFFIX;
		try {
			Class groovyClass = groovyCl.parseClass(new File(redisHandlerPath.getAbsolutePath(), className));
			RedisHandler handler = (RedisHandler) groovyClass.newInstance();
			if (logger.isDebugEnabled()) {
				logger.debug("Redis handler init success. handler=" + handler);
			}
			return handler;
		} catch (Exception e) {
			throw new RedisHandleException("Create groovy redis handler error:", e);
		}
	}

	public static String content(String name) {
		if (name == null || name.isEmpty()) {
			return null;
		}
		name = name.trim();
		String className = name.substring(0, 1).toUpperCase() + name.substring(1) + GROOVY_SUFFIX;
		StringBuilder content = new StringBuilder();
		FileInputStream file = null;
		BufferedReader reader = null;
		try {
			file = new FileInputStream(redisHandlerPath.getAbsoluteFile() + "/" + className);
			reader = new BufferedReader(new InputStreamReader(file));
			String line;
			while ((line = reader.readLine()) != null) {
				content.append(line).append("\r\n");
			}
		} catch (Exception e) {
			logger.error("Get redis handler content error:", e);
			return "Get redis handler content error:" + e.getMessage();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
				}
			}
			if (file != null) {
				try {
					file.close();
				} catch (IOException e) {
				}
			}
		}

		return content.toString();
	}

	public static boolean update(String name, String content) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Handler name can not be null or empty.");
        }
        name = name.trim();

        groovyCl.parseClass(content);

        String className = name.substring(0, 1).toUpperCase() + name.substring(1) + GROOVY_SUFFIX;
        FileWriter fileWriter = null;
        try {
            File file = new File(redisHandlerPath.getAbsoluteFile(), className);
            if (file.exists()) {
                file.delete();
                file.createNewFile();
            }
            file.canWrite();
            fileWriter = new FileWriter(file, true);
            fileWriter.write(content);
            fileWriter.flush();
        } catch (Exception e) {
            logger.error("Update handler:" + name + " content:" + content, e);
            return false;
        } finally {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                }
            }
        }
        return true;
    }
}
