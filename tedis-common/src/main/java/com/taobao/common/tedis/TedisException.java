/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-4-24 ÉÏÎç11:24:46
 * @version 1.0
 */
public class TedisException extends RuntimeException {

    private static final long serialVersionUID = -1611101501690221263L;

    public TedisException(String message) {
        super(message);
    }

    public TedisException(Throwable e) {
        super(e);
    }

    public TedisException(String message, Throwable cause) {
        super(message, cause);
    }
}
