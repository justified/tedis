/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.serializer.benchmark;

import java.util.Calendar;

import org.junit.Test;

import com.taobao.common.tedis.commands.benchmark.ItemData;
import com.taobao.common.tedis.serializer.HessianSerializer;
import com.taobao.common.tedis.serializer.JDKSerializer;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-8-4 下午06:42:52
 * @version 1.0
 */
public class SerializerTest {

    private static final int TOTAL_OPERATIONS = 100000;

    final ItemData itemData = new ItemData();

    @Test
    public void hessianBenchmark() {
        HessianSerializer serializer = new HessianSerializer();
        byte[] byteData = serializer.serialize(itemData);

        System.out.print("Hessian Serializer Benchmark Test");
        System.out.println("(ItemData with byte[] size:" + byteData.length + ").");

        long begin = Calendar.getInstance().getTimeInMillis();

        for (int n = 0; n <= TOTAL_OPERATIONS; n++) {
            serializer.serialize(itemData);
        }

        long elapsed = Calendar.getInstance().getTimeInMillis() - begin;

        System.out.println("serialize   ops:" + ((1000 * TOTAL_OPERATIONS) / elapsed));

        begin = Calendar.getInstance().getTimeInMillis();

        for (int n = 0; n <= TOTAL_OPERATIONS; n++) {
            serializer.deserialize(byteData);
        }

        elapsed = Calendar.getInstance().getTimeInMillis() - begin;

        System.out.println("deserialize ops:" + ((1000 * TOTAL_OPERATIONS) / elapsed));
    }

    @Test
    public void jdkBenchmark() {
        JDKSerializer serializer = new JDKSerializer();
        byte[] byteData = serializer.serialize(itemData);

        System.out.print("JDK Serializer Benchmark Test");
        System.out.println("(ItemData with byte[] size:" + byteData.length + ").");

        long begin = Calendar.getInstance().getTimeInMillis();

        for (int n = 0; n <= TOTAL_OPERATIONS; n++) {
            serializer.serialize(itemData);
        }

        long elapsed = Calendar.getInstance().getTimeInMillis() - begin;

        System.out.println("serialize   ops:" + ((1000 * TOTAL_OPERATIONS) / elapsed));

        begin = Calendar.getInstance().getTimeInMillis();

        for (int n = 0; n <= TOTAL_OPERATIONS; n++) {
            serializer.deserialize(byteData);
        }

        elapsed = Calendar.getInstance().getTimeInMillis() - begin;

        System.out.println("deserialize ops:" + ((1000 * TOTAL_OPERATIONS) / elapsed));
    }
    
    @Test
    public void size() {
        ItemData item = new ItemData();
        HessianSerializer serializer = new HessianSerializer();
        byte[] bytes = serializer.serialize(item);
        System.out.println(bytes.length);
    }

}
