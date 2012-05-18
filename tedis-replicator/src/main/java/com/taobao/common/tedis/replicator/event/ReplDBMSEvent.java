/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.event;

import java.sql.Timestamp;
import java.util.ArrayList;

import com.taobao.common.tedis.replicator.data.DBMSData;

public class ReplDBMSEvent extends ReplEvent implements ReplDBMSHeader {
	static final long serialVersionUID = 1300;

	long seqno;
	short fragno;
	boolean lastFrag;
	Timestamp extractedTstamp;
	String sourceId;
	long epochNumber;
	DBMSEvent event;

	public ReplDBMSEvent(long seqno, short fragno, boolean lastFrag, String sourceId, long epochNumber, Timestamp extractedTstamp, DBMSEvent event) {
		// All fields must exist to protect against failures. We therefore
		// validate object instances.
		this.seqno = seqno;
		this.fragno = fragno;
		this.lastFrag = lastFrag;
		this.epochNumber = epochNumber;
		if (sourceId == null)
			this.sourceId = "NONE";
		else
			this.sourceId = sourceId;
		if (extractedTstamp == null)
			this.extractedTstamp = new Timestamp(System.currentTimeMillis());
		else
			this.extractedTstamp = extractedTstamp;
		if (event == null)
			this.event = new DBMSEvent();
		else
			this.event = event;
	}

	public ReplDBMSEvent(long seqno, DBMSEvent event) {
		this(seqno, (short) 0, true, "NONE", 0, new Timestamp(System.currentTimeMillis()), event);
	}

	public long getSeqno() {
		return seqno;
	}

	public short getFragno() {
		return fragno;
	}

	public boolean getLastFrag() {
		return lastFrag;
	}

	public ArrayList<DBMSData> getData() {
		if (event != null)
			return event.getData();
		else
			return new ArrayList<DBMSData>();
	}

	public String getSourceId() {
		return sourceId;
	}

	public long getEpochNumber() {
		return epochNumber;
	}

	public Timestamp getExtractedTstamp() {
		return extractedTstamp;
	}

	public String getEventId() {
		return event.getEventId();
	}

	public void setShardId(String shardId) {
		this.getDBMSEvent().setMetaDataOption(ReplOptionParams.SHARD_ID, shardId);
	}

	public DBMSEvent getDBMSEvent() {
		return event;
	}

	@Override
	public String toString() {
		return "ReplDBMSEvent [seqno=" + seqno + ", extractedTstamp=" + extractedTstamp + ", event=" + event + "]";
	}
}