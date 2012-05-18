/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.manage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface ServiceManager {

	Map<String, String> redisHandlers();

	String redisHandlerContent(String handler);
	
	boolean updateRedisHandler(String name, String content);

	boolean configure(String serviceName, HashMap<String, String> props) throws Exception;

	void updatePropertyFile(String serviceName, Map<String, String> props);

	List<Map<String, String>> services() throws Exception;

	List<Map<String, String>> status() throws Exception;

	Map<String, String> status(String serviceName) throws Exception;

	boolean startService(String serviceName) throws Exception;

	boolean stopService(String serviceName) throws Exception;

	boolean online(String serviceName) throws Exception;

	boolean offline(String serviceName) throws Exception;

	void go() throws Exception;

	void restart() throws Exception;

}
