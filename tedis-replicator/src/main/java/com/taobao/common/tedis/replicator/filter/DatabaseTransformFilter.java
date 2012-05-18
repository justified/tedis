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

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.data.DBMSData;
import com.taobao.common.tedis.replicator.data.OneRowChange;
import com.taobao.common.tedis.replicator.data.RowChangeData;
import com.taobao.common.tedis.replicator.data.StatementData;
import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.plugin.PluginContext;

public class DatabaseTransformFilter implements Filter {
    private static Logger logger = Logger.getLogger(LoggingFilter.class);

    private String fromRegex;
    private String toRegex;

    Pattern pattern;
    Matcher matcher;

    /** Sets the regex used to match the database name. */
    public void setFromRegex(String fromRegex) {
        this.fromRegex = fromRegex;
    }

    /** Sets the corresponding regex to transform the name. */
    public void setToRegex(String toRegex) {
        this.toRegex = toRegex;
    }

    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException {
        ArrayList<DBMSData> data = event.getData();
        for (DBMSData dataElem : data) {
            if (dataElem instanceof RowChangeData) {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (OneRowChange orc : rdata.getRowChanges()) {
                    matcher.reset(orc.getSchemaName());
                    if (matcher.matches()) {
                        String oldSchema = orc.getSchemaName();
                        orc.setSchemaName(matcher.replaceAll(matcher.replaceAll(toRegex)));
                        if (logger.isDebugEnabled())
                            logger.debug("Filtered event schema name: old=" + oldSchema + " new=" + orc.getSchemaName());
                    }
                }
            } else if (dataElem instanceof StatementData) {
                StatementData sdata = (StatementData) dataElem;
                String schema = sdata.getDefaultSchema();
                if (schema == null)
                    continue;
                matcher.reset(schema);
                if (matcher.matches()) {
                    String oldSchema = schema;
                    sdata.setDefaultSchema(matcher.replaceAll(matcher.replaceAll(toRegex)));
                    if (logger.isDebugEnabled())
                        logger.debug("Filtered event schema name: old=" + oldSchema + " new=" + sdata.getDefaultSchema());
                }
            }
        }
        return event;
    }

    public void configure(PluginContext context) throws ReplicatorException {
        if (fromRegex == null)
            throw new ReplicatorException("fromRegex property must be set for regex filter to work");
        if (toRegex == null)
            throw new ReplicatorException("toRegex property must be set for regex filter to work");
    }

    public void prepare(PluginContext context) throws ReplicatorException {
        // Compile the pattern used for matching.
        try {
            pattern = Pattern.compile(fromRegex);
            matcher = pattern.matcher("");
        } catch (PatternSyntaxException e) {
            throw new ReplicatorException("Replicator fromRegex is invalid:  expression=" + fromRegex, e);
        }
    }

    public void release(PluginContext context) throws ReplicatorException {
    }
}
