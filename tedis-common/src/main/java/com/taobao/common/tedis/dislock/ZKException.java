/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.dislock;


public class ZKException extends Exception {

    private static final long serialVersionUID = -4226673647916172047L;

    public ZKException() {
        super();
    }

    public ZKException(String message, Throwable cause) {
        super(message, cause);
    }

    public ZKException(String message) {
        super(message);
    }

    public ZKException(Throwable cause) {
        super(cause);
    }

}


