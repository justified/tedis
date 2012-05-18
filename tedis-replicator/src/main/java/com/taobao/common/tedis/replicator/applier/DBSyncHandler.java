/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.applier;


/**
 * 一个handler只处理一张表
 *
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-2-27 上午10:12:18
 * @version 1.0
 */
public interface DBSyncHandler {

	/**
	 * @throws Exception
	 */
	void init() throws Exception;

	/**
	 * @param dataEvent
	 * @throws Exception
	 */
	void process(DataEvent dataEvent) throws Exception;

	/**
	 * @throws Exception
	 */
	void release() throws Exception;

	/**
	 * one table
	 * @return
	 */
	String interest();

}
