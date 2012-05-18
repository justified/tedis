/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import com.taobao.common.tedis.replicator.extractor.ExtractorException;

public class MySQLExtractException extends ExtractorException {
    private static final long serialVersionUID = 1L;

    public MySQLExtractException(String message) {
        super(message);
    }

    public MySQLExtractException(Throwable cause) {
        super(cause);
    }

    public MySQLExtractException(String message, Throwable cause) {
        super(message, cause);
    }

    public MySQLExtractException(String message, Throwable cause, String eventId) {
        super(message, cause);
    }
}
