/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.binary;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-8-1 09:40:34
 * @version 1.0
 */
public enum DataType {

    NONE("none"), VALUE("string"), LIST("list"), SET("set"), ZSET("zset"), HASH("hash");

    private static final Map<String, DataType> codeLookup = new ConcurrentHashMap<String, DataType>(6);

    static {
        for (DataType type : EnumSet.allOf(DataType.class))
            codeLookup.put(type.code, type);

    }

    private final String code;

    DataType(String name) {
        this.code = name;
    }

    public String code() {
        return code;
    }

    public static DataType fromCode(String code) {
        DataType data = codeLookup.get(code);
        if (data == null)
            throw new IllegalArgumentException("unknown data type code");
        return data;
    }
}
