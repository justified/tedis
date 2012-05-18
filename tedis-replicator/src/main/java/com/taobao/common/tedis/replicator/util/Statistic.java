/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.util;

public interface Statistic<T> {
    public void setValue(Number value);

    public T increment();

    public T add(Number value);

    public T decrement();

    public T subtract(Number value);

    public T getValue();

    public T getAverage();

    public String getLabel();

    public void clear();

}
