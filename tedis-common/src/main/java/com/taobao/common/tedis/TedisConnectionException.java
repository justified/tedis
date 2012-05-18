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
 * Tedis连接出现问题，会抛出此异常
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-4-24 上午11:23:59
 * @version 1.0
 */
public class TedisConnectionException extends TedisException {
    private static final long serialVersionUID = 6991632939402705878L;

    public TedisConnectionException(String message) {
        super(message);
    }

    public TedisConnectionException(Throwable cause) {
        super(cause);
    }

    public TedisConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
