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

public class WriteRowsLogEvent extends RowsLogEvent {

    public WriteRowsLogEvent(byte[] buffer, int eventLength, FormatDescriptionLogEvent descriptionEvent, boolean useBytesForString) throws ReplicatorException {
        super(buffer, eventLength, descriptionEvent, MysqlBinlog.WRITE_ROWS_EVENT, useBytesForString);
    }

    @Override
    public void processExtractedEvent(RowChangeData rowChanges, TableMapLogEvent map) throws ReplicatorException {
        if (map == null) {
            throw new MySQLExtractException("Write row event for unknown table");
        }
        OneRowChange oneRowChange = new OneRowChange();
        oneRowChange.setSchemaName(map.getDatabaseName());
        oneRowChange.setTableName(map.getTableName());
        oneRowChange.setTableId(map.getTableId());
        oneRowChange.setAction(RowChangeData.ActionType.INSERT);

        int rowIndex = 0; /* index of the row in value arrays */

        for (int bufferIndex = 0; bufferIndex < bufferSize;) {
            int length;

            /* Extract data */
            try {
                length = processExtractedEventRow(oneRowChange, rowIndex, usedColumns, bufferIndex, packedRowsBuffer, map, false);
            } catch (ReplicatorException e) {
                logger.error("Failure while processing extracted write row event", e);
                throw (e);
            }
            rowIndex++;

            if (length == 0)
                break;
            bufferIndex += length;
        }
        rowChanges.appendOneRowChange(oneRowChange);
    }

}
