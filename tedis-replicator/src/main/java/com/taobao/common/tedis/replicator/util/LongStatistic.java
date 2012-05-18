/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.util;

public class LongStatistic implements Statistic<Long> {
    String label;
    Long value = new Long(0);
    long count;

    public LongStatistic(String label) {
        this.label = label;
    }

    public Long decrement() {
        return value -= 1;
    }

    public Long getAverage() {
        if (count > 0) {
            return value / count;
        }

        return value;
    }

    public String getLabel() {
        return label;
    }

    public Long getValue() {
        return value;
    }

    public Long increment() {
        return value += 1;
    }

    public void setValue(Number value) {
        this.value = value.longValue();
    }

    public Long add(Number value) {
        return this.value += value.longValue();
    }

    public Long subtract(Number value) {
        return this.value -= value.longValue();
    }

    public String toString() {
        return value.toString();
    }

    public void clear() {
        value = new Long(0);
        count = 0;
    }
}
