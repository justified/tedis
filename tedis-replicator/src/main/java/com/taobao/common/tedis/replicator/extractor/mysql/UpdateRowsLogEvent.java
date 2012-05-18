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

public class UpdateRowsLogEvent extends RowsLogEvent {

    public UpdateRowsLogEvent(byte[] buffer, int eventLength, FormatDescriptionLogEvent descriptionEvent, boolean useBytesForString) throws ReplicatorException {
        super(buffer, eventLength, descriptionEvent, MysqlBinlog.UPDATE_ROWS_EVENT, useBytesForString);
    }

    @Override
    public void processExtractedEvent(RowChangeData rowChanges, TableMapLogEvent map) throws ReplicatorException {
        /**
         * For UPDATE_ROWS_LOG_EVENT, a row matching the first row-image is
         * removed, and the row described by the second row-image is inserted.
         */
        if (map == null) {
            throw new MySQLExtractException("Update row event for unknown table");
        }
        OneRowChange oneRowChange = new OneRowChange();
        oneRowChange.setSchemaName(map.getDatabaseName());
        oneRowChange.setTableName(map.getTableName());
        oneRowChange.setTableId(map.getTableId());
        oneRowChange.setAction(RowChangeData.ActionType.UPDATE);

        int rowIndex = 0; /* index of the row in value arrays */

        int bufferIndex = 0;
        while (bufferIndex < bufferSize) {
            int length = 0;

            try {
                /*
                 * Removed row
                 */
                length = processExtractedEventRow(oneRowChange, rowIndex, usedColumns, bufferIndex, packedRowsBuffer, map, true);

                if (length == 0)
                    break;

                bufferIndex += length;
                /*
                 * Inserted row
                 */
                length = processExtractedEventRow(oneRowChange, rowIndex, usedColumnsForUpdate, bufferIndex, packedRowsBuffer, map, false);
            } catch (ReplicatorException e) {
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
