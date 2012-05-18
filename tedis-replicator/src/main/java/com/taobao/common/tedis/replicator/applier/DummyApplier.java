/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.applier;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.data.DBMSData;
import com.taobao.common.tedis.replicator.data.OneRowChange;
import com.taobao.common.tedis.replicator.data.RowChangeData;
import com.taobao.common.tedis.replicator.data.StatementData;
import com.taobao.common.tedis.replicator.data.OneRowChange.ColumnSpec;
import com.taobao.common.tedis.replicator.data.OneRowChange.ColumnVal;
import com.taobao.common.tedis.replicator.event.DBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSFilteredEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.plugin.PluginContext;

public class DummyApplier implements RawApplier {

    ReplDBMSHeader lastHeader;

    @Override
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit, boolean doRollback) throws ReplicatorException, InterruptedException {
        for (DBMSData dataElem : event.getData()) {
            if (dataElem instanceof RowChangeData) {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (OneRowChange orc : rdata.getRowChanges()) {
                    List<ColumnSpec> keyspec = orc.getKeySpec();
                    System.out.println("keyspec:");
                    for(ColumnSpec s : keyspec) {
                        System.out.println(s.getName());
                    }
                    ArrayList<ArrayList<ColumnVal>> keyvalues = orc.getKeyValues();
                    System.out.println("keyvalues:");
                    for(ArrayList<ColumnVal> list : keyvalues) {
                        for(ColumnVal v: list) {
                            System.out.println(v.getValue());
                        }
                    }
                    List<ColumnSpec> columnspec = orc.getColumnSpec();
                    System.out.println("columnspec:");
                    for(ColumnSpec s : columnspec) {
                        System.out.println(s.getName());
                    }
                    ArrayList<ArrayList<ColumnVal>> colomvalues = orc.getColumnValues();
                    System.out.println("columnvalues:");
                    for(ArrayList<ColumnVal> list : colomvalues) {
                        for(ColumnVal v : list) {
                            System.out.println(v.getValue());
                        }
                    }
                }
            } else if (dataElem instanceof StatementData) {
                StatementData sdata = (StatementData) dataElem;
                System.out.println(sdata.getDefaultSchema() + "->" + sdata);
            }
        }
        if (doCommit) {
            lastHeader = header;
        }
    }

    @Override
    public void commit() throws ReplicatorException, InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public ReplDBMSHeader getLastEvent() throws ReplicatorException, InterruptedException {
        lastHeader = new ReplDBMSFilteredEvent(0l, (short)0, 0l, (short)0, true, "000001:0", "localhost", new Timestamp(System.currentTimeMillis()));
        return null;
    }

    @Override
    public void rollback() throws InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTaskId(int id) {
        // TODO Auto-generated method stub

    }

    @Override
    public void configure(PluginContext context) throws ReplicatorException, InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void prepare(PluginContext context) throws ReplicatorException, InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void release(PluginContext context) throws ReplicatorException, InterruptedException {
        // TODO Auto-generated method stub

    }

}
