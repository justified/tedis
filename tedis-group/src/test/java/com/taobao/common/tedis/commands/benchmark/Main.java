/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.commands.benchmark;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

public class Main {
    public static void main(String[] args) {
//        System.out.println("============start tair benchmark=================");
//        Result result = JUnitCore.runClasses(TairBenchMark.class);
//        System.out.println("runtime:" + result.getRunTime());
//        System.out.println("runcount:" + result.getRunCount());
//        System.out.println("failurecout:" + result.getFailureCount());
//        System.out.println("============ended tair benchmark=================");

        System.out.println("============start tedis benchmark================");
        Result result = JUnitCore.runClasses(BenchmarkTest.class);
        System.out.println("runtime:" + result.getRunTime());
        System.out.println("runcount:" + result.getRunCount());
        System.out.println("failurecout:" + result.getFailureCount());
        System.out.println("============ended tedis benchmark================");
    }
}
