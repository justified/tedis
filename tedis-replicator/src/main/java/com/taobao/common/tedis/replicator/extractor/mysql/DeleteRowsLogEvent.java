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
import com.taobao.common.tedis.replicator.data.OneRowChange;
import com.taobao.common.tedis.replicator.data.RowChangeData;
import com.taobao.common.tedis.replicator.extractor.ExtractorException;

public class DeleteRowsLogEvent extends RowsLogEvent {

    public DeleteRowsLogEvent(byte[] buffer, int eventLength, FormatDescriptionLogEvent descriptionEvent, boolean useBytesForString) throws ReplicatorException {
        super(buffer, eventLength, descriptionEvent, MysqlBinlog.DELETE_ROWS_EVENT, useBytesForString);
    }

    @Override
    public void processExtractedEvent(RowChangeData rowChanges, TableMapLogEvent map) throws ReplicatorException {
        if (map == null) {
            throw new MySQLExtractException("Delete row event for unknown table");
        }
        OneRowChange oneRowChange = new OneRowChange();
        oneRowChange.setSchemaName(map.getDatabaseName());
        oneRowChange.setTableName(map.getTableName());
        oneRowChange.setTableId(map.getTableId());
        oneRowChange.setAction(RowChangeData.ActionType.DELETE);

        int rowIndex = 0; /* index of the row in value arrays */

        for (int i = 0; i < bufferSize;) {
            int length = 0;

            try {
                length = processExtractedEventRow(oneRowChange, rowIndex, usedColumns, i, packedRowsBuffer, map, true);
            } catch (ExtractorException e) {
                throw (e);
            }
            rowIndex++;

            if (length == 0)
                break;
            i += length;
        }
        rowChanges.appendOneRowChange(oneRowChange);
    }
}
