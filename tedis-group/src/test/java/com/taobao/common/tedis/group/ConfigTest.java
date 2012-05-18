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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.taobao.common.tedis.config.HAConfig;
import com.taobao.common.tedis.group.DiamondConfigManager;

/**
 *
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class ConfigTest {

    public ConfigTest() {
    }


    @Test
    public void testConfigParse(){
       HAConfig conf =  DiamondConfigManager.parseConfig("servers=192.168.0.1:6739:r10,192.168.0.2:6739:r10;timeout=2000;pool_size=10;password=yesorno;");
       assertEquals(conf.pool_size, 10);
       assertEquals(conf.timeout, 2000);
       assertEquals(conf.groups.size(), 2);
       assertEquals(conf.password, "yesorno");
    }

}
