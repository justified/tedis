/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.event;

import java.io.Serializable;

public class ReplOption implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name = "";
    private String value = "";

    public ReplOption(String option, String value) {
        this.name = option;
        this.value = value;
    }

    public String getOptionName() {
        return name;
    }

    public String getOptionValue() {
        return value;
    }

    @Override
    public String toString() {
        return name + " = " + value;
    }
}
