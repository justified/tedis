/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.group;

import com.taobao.common.tedis.group.TedisGroup;

public class RetryConcurrentTest {

	static TedisGroup group = new TedisGroup("test", "v0");

	public static void main(String[] args) {
		new TestThread().start();
		group.getTedis().get("b".getBytes());

	}

	static class TestThread extends Thread {

		@Override
		public void run() {
			group.getTedis().get("a".getBytes());
		}

	}
}
