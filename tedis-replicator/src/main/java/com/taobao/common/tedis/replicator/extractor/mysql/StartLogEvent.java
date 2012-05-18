/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import com.taobao.common.tedis.replicator.ReplicatorException;

public class StartLogEvent extends LogEvent {

    public StartLogEvent() {
        super();
    }

    public StartLogEvent(byte[] buffer, FormatDescriptionLogEvent descriptionEvent) throws ReplicatorException {
        super(buffer, descriptionEvent, MysqlBinlog.START_EVENT_V3);
    }
}
