/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.PARAMETER })
public @interface ShardKey {
    public static enum Type {
        SINGLE, MULTI, MAP, ALL, RT_OBOJECT, RT_SET, RT_MAP, RT_LIST;
    }

    Type value() default Type.SINGLE;

    Type retType() default Type.RT_OBOJECT;
}
