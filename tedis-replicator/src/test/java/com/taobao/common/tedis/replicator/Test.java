/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator;

import java.sql.Timestamp;

import com.taobao.common.tedis.commands.TedisManagerFactory;
import com.taobao.common.tedis.core.HashCommands;
import com.taobao.common.tedis.core.TedisManager;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-5-15
 * @version 1.0
 */
public class Test {
    public static void main(String[] args) {
        TedisManager tedisManager = TedisManagerFactory.create("ju", "v0");
        HashCommands hashCommands = tedisManager.getHashCommands();
        int namespace = 123;
        hashCommands.put(namespace, "dbsync_lastheader_snsju-item-itemdetail_0", "epoch_number", 0L);
        hashCommands.put(namespace, "dbsync_lastheader_snsju-item-itemdetail_0", "fragno", 0);
        hashCommands.put(namespace, "dbsync_lastheader_snsju-item-itemdetail_0", "task_id", 0);
        hashCommands.put(namespace, "dbsync_lastheader_snsju-item-itemdetail_0", "seqno", 3875475L);
        hashCommands.put(namespace, "dbsync_lastheader_snsju-item-itemdetail_0", "source_id", "localhost");
        hashCommands.put(namespace, "dbsync_lastheader_snsju-item-itemdetail_0", "extracted_timestamp", Timestamp.valueOf("2012-05-15 10:35:32.0"));
        hashCommands.put(namespace, "dbsync_lastheader_snsju-item-itemdetail_0", "event_id", "000330:00473972052;0");
        hashCommands.put(namespace, "dbsync_lastheader_snsju-item-itemdetail_0", "last_frag", true);
    }
}
