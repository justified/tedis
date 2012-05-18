/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.applier;

import java.io.Serializable;
import java.util.Map;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.data.RowChangeData.ActionType;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;

public class DataEvent implements Serializable {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = Logger.getLogger(DataEvent.class);

	private ActionType actionType;

	private Map<String, Object> data;

	private String schema;

	private String table;

	private ReplDBMSHeader header;

	private long time;

	private boolean commit;

	public DataEvent(ActionType actionType, Map<String, Object> data, String schema, String table, ReplDBMSHeader header, long time, boolean commit) {
		this.actionType = actionType;
		this.data = data;
		this.schema = schema;
		this.table = table;
		this.header = header;
		this.time = time;
		this.commit = commit;
	}

	public ActionType getActionType() {
		return actionType;
	}

	public void setActionType(ActionType actionType) {
		this.actionType = actionType;
	}

	public Map<String, Object> getData() {
		return data;
	}

	public void setData(Map<String, Object> data) {
		this.data = data;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public int getTableIndex() {
		StringBuilder sb = new StringBuilder();
		for (int i = table.length() - 1; i >= 0; i--) {
			char c = table.charAt(i);
			if (c >= '0' && c <= '9') {
				sb.append(c);
			} else {
				break;
			}
		}
		sb = sb.reverse();
		if (sb.length() > 0) {
			return Integer.parseInt(sb.toString());
		} else {
			if (logger.isDebugEnabled()) {
				logger.debug("Table is not sharded? check the table name : " + table);
			}
			return table.hashCode();
		}
	}

	public ReplDBMSHeader getHeader() {
		return header;
	}

	public void setHeader(ReplDBMSHeader header) {
		this.header = header;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public boolean isCommit() {
		return commit;
	}

	public void setCommit(boolean commit) {
		this.commit = commit;
	}

	@Override
	public String toString() {
		return "DataEvent [actionType=" + actionType + ", data=" + data + ", schema=" + schema + ", table=" + table + ", eventId=" + header + ", time=" + time + "]";
	}

}
