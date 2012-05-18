/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.util;

public class DoubleStatistic implements Statistic<Double> {
    String label;
    Double value = new Double(0);
    long count;

    public DoubleStatistic(String label) {
        this.label = label;
    }

    public Double decrement() {
        return value -= 1;
    }

    public Double getAverage() {
        if (count > 0) {
            return value / count;
        }

        return value;
    }

    public String getLabel() {
        return label;
    }

    public Double getValue() {
        return value;
    }

    public Double increment() {
        return value += 1;
    }

    public void setValue(Number value) {
        this.value = value.doubleValue();
    }

    public Double add(Number value) {
        count++;
        return this.value += value.longValue();
    }

    public Double subtract(Number value) {
        return this.value -= value.doubleValue();
    }

    public String toString() {
        return value.toString();
    }

    public void clear() {
        value = new Double(0);
        count = 0;
    }
}
