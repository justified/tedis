/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.event;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.taobao.common.tedis.replicator.data.DBMSData;

public class DBMSEvent implements Serializable {
    private static final long serialVersionUID = 1300L;
    private String id;
    private LinkedList<ReplOption> metadata;
    private ArrayList<DBMSData> data;
    private boolean lastFrag;
    private Timestamp sourceTstamp;
    private LinkedList<ReplOption> options;

    public DBMSEvent(String id, LinkedList<ReplOption> metadata, ArrayList<DBMSData> data, boolean lastFrag, Timestamp sourceTstamp) {
        // Eliminate all possibilities of null pointers.
        if (id == null)
            this.id = "NIL";
        else
            this.id = id;
        if (metadata == null)
            this.metadata = new LinkedList<ReplOption>();
        else
            this.metadata = metadata;
        if (data == null)
            this.data = new ArrayList<DBMSData>();
        else
            this.data = data;
        this.lastFrag = lastFrag;
        if (sourceTstamp == null)
            this.sourceTstamp = new Timestamp(System.currentTimeMillis());
        else
            this.sourceTstamp = sourceTstamp;
        options = new LinkedList<ReplOption>();
    }

    public DBMSEvent(String id, ArrayList<DBMSData> data, Timestamp sourceTstamp) {
        this(id, new LinkedList<ReplOption>(), data, true, sourceTstamp);
    }

    public DBMSEvent(String id, ArrayList<DBMSData> data, boolean lastFrag, Timestamp sourceTstamp) {
        this(id, new LinkedList<ReplOption>(), data, lastFrag, sourceTstamp);
    }

    public DBMSEvent(String id, LinkedList<ReplOption> metadata, ArrayList<DBMSData> data, Timestamp sourceTstamp) {
        this(id, metadata, data, true, sourceTstamp);
    }

    public DBMSEvent(String id) {
        this(id, null, null, true, null);
    }

    public DBMSEvent() {
        this(null, null, null, true, null);
    }

    public String getEventId() {
        return id;
    }

    public LinkedList<ReplOption> getMetadata() {
        return metadata;
    }

    public void addMetadataOption(String name, String value) {
        metadata.add(new ReplOption(name, value));
    }

    public void setMetaDataOption(String name, String value) {
        for (int i = 0; i < metadata.size(); i++) {
            ReplOption option = metadata.get(i);
            if (name.equals(option.getOptionName())) {
                metadata.set(i, new ReplOption(name, value));
                return;
            }
        }
        addMetadataOption(name, value);
    }

    public ReplOption getMetadataOption(String name) {
        for (ReplOption option : metadata) {
            if (name.equals(option.getOptionName()))
                return option;
        }
        return null;
    }

    public String getMetadataOptionValue(String name) {
        for (ReplOption option : metadata) {
            if (name.equals(option.getOptionName()))
                return option.getOptionValue();
        }
        return null;
    }

    public ArrayList<DBMSData> getData() {
        return data;
    }

    public boolean isLastFrag() {
        return lastFrag;
    }

    public Timestamp getSourceTstamp() {
        return sourceTstamp;
    }

    public void setOptions(LinkedList<ReplOption> savedOptions) {
        this.options.addAll(savedOptions);
    }

    public List<ReplOption> getOptions() {
        return options;
    }

    public void addOption(String name, String value) {
        options.add(new ReplOption(name, value));
    }

	@Override
	public String toString() {
		return "DBMSEvent [id=" + id + ", data=" + data + ", sourceTstamp=" + sourceTstamp + "]";
	}

}
