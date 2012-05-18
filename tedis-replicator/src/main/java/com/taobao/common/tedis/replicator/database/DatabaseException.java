/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

import com.taobao.common.tedis.replicator.ReplicatorException;

public class DatabaseException extends ReplicatorException {
    static final long serialVersionUID = 1L;

    public DatabaseException() {
        super();
    }

    public DatabaseException(String msg) {
        super(msg);
    }

    public DatabaseException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public DatabaseException(Throwable cause) {
        super(cause);
    }

}
