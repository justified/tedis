/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.data;

import java.util.ArrayList;
import java.util.LinkedList;

import com.taobao.common.tedis.replicator.event.ReplOption;

public class RowChangeData extends DBMSData {
    public enum ActionType {
        INSERT, DELETE, UPDATE
    }

    private static final long serialVersionUID = 1L;
    private ArrayList<OneRowChange> rowChanges;
    private LinkedList<ReplOption> options = new LinkedList<ReplOption>();

    public RowChangeData() {
        super();
        rowChanges = new ArrayList<OneRowChange>();
    }

    public ArrayList<OneRowChange> getRowChanges() {
        return rowChanges;
    }

    public void setRowChanges(ArrayList<OneRowChange> rowChanges) {
        this.rowChanges = rowChanges;
    }

    public void appendOneRowChange(OneRowChange rowChange) {
        this.rowChanges.add(rowChange);
    }

    public void addOptions(LinkedList<ReplOption> savedOptions) {
        this.options.addAll(savedOptions);
    }

    public LinkedList<ReplOption> getOptions() {
        return options;
    }

}
