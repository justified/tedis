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
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.conf.ReplicatorConf;
import com.taobao.common.tedis.replicator.data.DBMSData;
import com.taobao.common.tedis.replicator.data.OneRowChange;
import com.taobao.common.tedis.replicator.data.RowChangeData;
import com.taobao.common.tedis.replicator.data.StatementData;
import com.taobao.common.tedis.replicator.database.SqlOperation;
import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.plugin.PluginContext;

public class ReplicateFilter implements Filter {

    private static Logger logger = Logger.getLogger(ReplicateFilter.class);

    private Pattern doDbPattern;
    private Matcher doDbMatcher;

    private Pattern ignoreDbPattern;
    private Matcher ignoreDbMatcher;

    private Pattern doTablePattern;
    private Matcher doTableMatcher;

    private Pattern ignoreTablePattern;
    private Matcher ignoreTableMatcher;

    private Matcher ignoreWildTableMatcher;
    private Matcher doWildTableMatcher;

    private String doFilter;
    private String ignoreFilter;
    private Pattern doWildTablePattern;
    private Pattern ignoreWildTablePattern;

    private String tedisSchema;

    public void setDoFilter(String doFilter) {
        this.doFilter = doFilter;
    }

    public void setIgnoreFilter(String ignoreFilter) {
        this.ignoreFilter = ignoreFilter;

    }

    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException, InterruptedException {
        ArrayList<DBMSData> data = event.getData();

        if (data == null)
            return event;

        for (Iterator<DBMSData> iterator = data.iterator(); iterator.hasNext();) {
            DBMSData dataElem = iterator.next();
            if (dataElem instanceof RowChangeData) {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (Iterator<OneRowChange> iterator2 = rdata.getRowChanges().iterator(); iterator2.hasNext();) {
                    OneRowChange orc = iterator2.next();

                    if (filterEvent(orc.getSchemaName(), orc.getTableName())) {
                        iterator2.remove();
                    }
                }
                if (rdata.getRowChanges().isEmpty()) {
                    iterator.remove();
                }
            } else if (dataElem instanceof StatementData) {
                StatementData sdata = (StatementData) dataElem;
                String schema = null;
                String table = null;

                Object parsingMetadata = sdata.getParsingMetadata();
                if (parsingMetadata != null && parsingMetadata instanceof SqlOperation) {
                    SqlOperation parsed = (SqlOperation) parsingMetadata;
                    schema = parsed.getSchema();
                    table = parsed.getName();
                    if (logger.isDebugEnabled())
                        logger.debug("Parsing found schema = " + schema + " / table = " + table);
                }

                if (schema == null)
                    schema = sdata.getDefaultSchema();

                if (schema == null) {
                    final String query = sdata.getQuery();
                    logger.warn("Ignoring event : No schema found for this event " + event.getSeqno() + (query != null ? " (" + query.substring(0, Math.min(query.length(), 200)) + "...)" : ""));
                    continue;
                }

                if (filterEvent(schema, table)) {
                    if (logger.isDebugEnabled())
                        logger.debug("Filtering event");
                    iterator.remove();
                }
            }
        }

        if (data.isEmpty()) {
            return null;
        }
        return event;
    }

    private boolean filterEvent(String schema, String table) {
        // if schema not provided, cannot filter
        if (schema.length() == 0)
            return false;

        if (schema.equals(tedisSchema))
            return false;

        if (doDbPattern != null) {
            if (logger.isDebugEnabled())
                logger.debug("Checking if database should be replicated : " + schema);
            if (doDbMatcher == null)
                doDbMatcher = doDbPattern.matcher(schema);
            else
                doDbMatcher.reset(schema);

            if (doDbMatcher.matches()) {
                if (logger.isDebugEnabled())
                    logger.debug("Match do filter - keeping event");
                return false;
            }
        }

        if (ignoreDbPattern != null) {
            if (ignoreDbMatcher == null)
                ignoreDbMatcher = ignoreDbPattern.matcher(schema);
            else
                ignoreDbMatcher.reset(schema);

            if (ignoreDbMatcher.matches())
                return true;
        }

        // From this point, if table not provided, cannot filter
        if (table != null && table.length() == 0)
            return false;

        String searchedTable = schema + "." + table;

        if (doTablePattern != null) {
            if (doTableMatcher == null)
                doTableMatcher = doTablePattern.matcher(searchedTable);
            else
                doTableMatcher.reset(searchedTable);

            if (doTableMatcher.matches())
                return false;
        }

        if (ignoreTablePattern != null) {
            if (ignoreTableMatcher == null)
                ignoreTableMatcher = ignoreTablePattern.matcher(searchedTable);
            else
                ignoreTableMatcher.reset(searchedTable);

            if (ignoreTableMatcher.matches())
                return true;
        }

        if (doWildTablePattern != null) {
            if (doWildTableMatcher == null)
                doWildTableMatcher = doWildTablePattern.matcher(searchedTable);
            else
                doWildTableMatcher.reset(searchedTable);

            if (doWildTableMatcher.matches())
                return false;
        }

        if (ignoreWildTablePattern != null) {
            if (ignoreWildTableMatcher == null)
                ignoreWildTableMatcher = ignoreWildTablePattern.matcher(searchedTable);
            else
                ignoreWildTableMatcher.reset(searchedTable);

            if (ignoreWildTableMatcher.matches())
                return true;
        }

        /*
         * At this point check whether the do filters were used or not : if they
         * were, then it means that the table/schema that was looked for did not
         * match any of the filters => drop the event. If they were not used
         * (ignore instead) then at this point, event should have been dropped
         * elsewhere if needed.
         */
        return doDbPattern != null || doTablePattern != null || doWildTablePattern != null;
    }

    public void configure(PluginContext context) throws ReplicatorException, InterruptedException {
        tedisSchema = context.getReplicatorProperties().getString(ReplicatorConf.METADATA_SCHEMA);
    }

    public void prepare(PluginContext context) throws ReplicatorException, InterruptedException {
        logger.warn("Preparing Replicate Filter");
        extractFilters(doFilter, false);
        extractFilters(ignoreFilter, true);

    }

    private void extractFilters(String doOrIgnorefilter, boolean ignore) {
        StringBuffer db = new StringBuffer("");
        StringBuffer table = new StringBuffer("");
        StringBuffer wildTable = new StringBuffer("");

        String[] filterArr = doOrIgnorefilter.split(",");

        for (int i = 0; i < filterArr.length; i++) {
            String filter = filterArr[i].trim();
            if (filter.length() == 0)
                continue;

            if (!filter.contains(".")) {
                // This is a schema
                if (db.length() > 0)
                    db.append("|");
                db.append(filter);
            } else {
                filter = filter.replace(".", "\\.");
                if (filter.contains("%") || filter.contains("_")) {
                    if (wildTable.length() > 0)
                        wildTable.append("|");
                    wildTable.append(filter.replaceAll("%", ".*").replaceAll("_", "."));
                } else {
                    if (table.length() > 0)
                        table.append("|");
                    table.append(filter);
                }
            }
            final String tablePattern = table.toString();
            final String wildTablePattern = wildTable.toString();
            final String dbPattern = db.toString();
            if (ignore) {
                ignoreDbPattern = Pattern.compile(dbPattern);
                ignoreTablePattern = Pattern.compile(tablePattern);
                ignoreWildTablePattern = Pattern.compile(wildTablePattern);
            } else {
                if (dbPattern.length() > 0)
                    doDbPattern = Pattern.compile(dbPattern);
                if (tablePattern.length() > 0)
                    doTablePattern = Pattern.compile(tablePattern);
                if (wildTablePattern.length() > 0)
                    doWildTablePattern = Pattern.compile(wildTablePattern);

            }
        }
    }

    public void release(PluginContext context) throws ReplicatorException, InterruptedException {

    }

}
