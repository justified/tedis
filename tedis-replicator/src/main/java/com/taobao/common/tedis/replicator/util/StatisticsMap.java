/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.util;

import java.util.Map;
import java.util.TreeMap;

public class StatisticsMap extends TreeMap<String, Statistic<?>> {
    private static final long serialVersionUID = 1L;
    private String name = null;

    public StatisticsMap(String name) {
        this.name = name;
    }

    public void addDoubleStatistic(String label) {
        put(label, new DoubleStatistic(label));
    }

    public void addLongStatistic(String label) {
        put(label, new LongStatistic(label));
    }

    @SuppressWarnings("unchecked")
    public Number increment(String label) {
        Statistic<Number> stat = (Statistic<Number>) get(label);
        if (stat == null) {
            return 0L;
        }
        return stat.increment();
    }

    @SuppressWarnings("unchecked")
    public Number decrement(String label) {
        Statistic<Number> stat = (Statistic<Number>) get(label);
        if (stat == null) {
            return 0L;
        }
        return stat.decrement();
    }

    @SuppressWarnings("unchecked")
    public Number add(String label, Number value) {
        Statistic<Number> stat = (Statistic<Number>) get(label);
        if (stat == null) {
            return 0L;
        }
        return stat.add(value);
    }

    @SuppressWarnings("unchecked")
    public Number subtract(String label, Number value) {
        Statistic<Number> stat = (Statistic<Number>) get(label);
        if (stat == null) {
            return 0L;
        }
        return stat.subtract(value);
    }

    @SuppressWarnings("unchecked")
    public Number getAverage(String label) {
        Statistic<Number> stat = (Statistic<Number>) get(label);
        if (stat == null) {
            return 0L;
        }
        return stat.getAverage();
    }

    @SuppressWarnings("unchecked")
    public Number getValue(String label) {
        Statistic<Number> stat = (Statistic<Number>) get(label);
        if (stat == null) {
            return 0L;
        }
        return stat.getValue();
    }

    public void clear(String label) {
        Statistic<?> stats = get(label);
        if (stats != null)
            stats.clear();
    }

    public Map<String, ?> getMap() {
        return this;
    }

    public String toString() {
        return new ResultFormatter(this).format();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
