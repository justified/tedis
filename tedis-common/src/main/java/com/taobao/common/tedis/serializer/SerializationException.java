/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.serializer;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 上午11:55:13
 * @version 1.0
 */
public class SerializationException extends RuntimeException {

    private static final long serialVersionUID = -8561411072499373859L;

    /**
     * Constructs a new <code>SerializationException</code> instance.
     * 
     * @param msg
     * @param cause
     */
    public SerializationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs a new <code>SerializationException</code> instance.
     * 
     * @param msg
     */
    public SerializationException(String msg) {
        super(msg);
    }
}
