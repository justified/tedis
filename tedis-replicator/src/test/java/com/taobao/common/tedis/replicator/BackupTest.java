/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.manage.ReplicatorServiceManager;
import com.taobao.common.tedis.replicator.manage.ServiceManager;

public class BackupTest {

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		ServiceManager serviceManager = ReplicatorServiceManager.getInstance("127.0.0.1:2181");
		serviceManager.go();
	}

}
