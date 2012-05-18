/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.filter;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.data.DBMSData;
import com.taobao.common.tedis.replicator.data.OneRowChange;
import com.taobao.common.tedis.replicator.data.RowChangeData;
import com.taobao.common.tedis.replicator.data.StatementData;
import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.plugin.PluginContext;

public class DatabaseFilter implements Filter {

    private String schema;

    Pattern pattern;
    Matcher matcher;

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException, InterruptedException {
        ArrayList<DBMSData> data = event.getData();
        for (DBMSData dataElem : data) {
            if (dataElem instanceof RowChangeData) {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (OneRowChange orc : rdata.getRowChanges()) {
                    matcher.reset(orc.getSchemaName());
                    if (matcher.matches()) {
                        return event;
                    } else {
                        return null;
                    }
                }
            } else if (dataElem instanceof StatementData) {
                StatementData sdata = (StatementData) dataElem;
                String schema = sdata.getDefaultSchema();
                if (schema == null)
                    continue;
                matcher.reset(schema);
                if (matcher.matches()) {
                    return event;
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    @Override
    public void configure(PluginContext context) throws ReplicatorException, InterruptedException {
        if (schema == null) {
            throw new ReplicatorException("Schema property must be set for regex filter to work");
        }
    }

    @Override
    public void prepare(PluginContext context) throws ReplicatorException, InterruptedException {
        try {
            pattern = Pattern.compile(schema);
            matcher = pattern.matcher("");
        } catch (PatternSyntaxException e) {
            throw new ReplicatorException("Replicator fromRegex is invalid:  expression=" + schema, e);
        }
    }

    @Override
    public void release(PluginContext context) throws ReplicatorException, InterruptedException {

    }

}
