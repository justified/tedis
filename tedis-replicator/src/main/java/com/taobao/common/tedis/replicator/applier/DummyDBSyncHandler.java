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
 *
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-5-15
 * @version 1.0
 */
public class DummyDBSyncHandler implements DBSyncHandler {

    /**
     * @throws Exception
     */
    @Override
    public void init() throws Exception {
        // TODO Auto-generated method stub

    }

    /**
     * @param dataEvent
     * @throws Exception
     */
    @Override
    public void process(DataEvent dataEvent) throws Exception {
        System.out.println(dataEvent);
    }

    /**
     * @throws Exception
     */
    @Override
    public void release() throws Exception {
        // TODO Auto-generated method stub

    }

    /**
     * @return
     */
    @Override
    public String interest() {
        return "item";
    }

}
