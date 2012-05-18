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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.commands.TedisManagerFactory;
import com.taobao.common.tedis.core.TedisManager;
import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.applier.RawApplier;
import com.taobao.common.tedis.replicator.data.DBMSData;
import com.taobao.common.tedis.replicator.data.LoadDataFileFragment;
import com.taobao.common.tedis.replicator.data.OneRowChange;
import com.taobao.common.tedis.replicator.data.OneRowChange.ColumnSpec;
import com.taobao.common.tedis.replicator.data.OneRowChange.ColumnVal;
import com.taobao.common.tedis.replicator.data.RowChangeData;
import com.taobao.common.tedis.replicator.data.RowChangeData.ActionType;
import com.taobao.common.tedis.replicator.data.RowIdData;
import com.taobao.common.tedis.replicator.data.StatementData;
import com.taobao.common.tedis.replicator.event.DBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeaderData;
import com.taobao.common.tedis.replicator.plugin.PluginContext;

public class DBSyncApplier implements RawApplier {

    private static final Logger logger = Logger.getLogger(DBSyncApplier.class);
    private static final Logger monitorLogger = MonitorLogger.logger;

    private int taskId;

    private TedisManager tedisManager;
    private String appName = "test";
    private String version = "v0";

    private int configNamespace = 0;
    private String configKey = "dbsync_lastheader";
    private String serviceName;

    private long waitTime = 3000;
    private int threadSize = 10;

    private final int waitingQueueLimit = 10000;
    private final int processingQueueLimit = 10000;

    private BlockingQueue<DataEvent> waitingQueue = new ArrayBlockingQueue<DataEvent>(waitingQueueLimit);
    private ArrayList<BlockingQueue<DataEvent>> processingQueues = new ArrayList<BlockingQueue<DataEvent>>(threadSize);

    private String handlers;
    private Map<String, DBSyncHandler> syncHandlers;

    private List<Thread> threadPool = new ArrayList<Thread>();
    private Thread waitingThread;

    @Override
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit, boolean doRollback) throws ReplicatorException, InterruptedException {
        ArrayList<DBMSData> dbmsDataValues = event.getData();
        for (DBMSData dbmsData : dbmsDataValues) {
            if (dbmsData instanceof StatementData) {
                logger.warn("Ignoring statement.");
            } else if (dbmsData instanceof RowChangeData) {
                RowChangeData rd = (RowChangeData) dbmsData;
                for (OneRowChange orc : rd.getRowChanges()) {
                    ActionType action = orc.getAction();
                    String schema = orc.getSchemaName();
                    String table = orc.getTableName();

                    boolean interested = false;
                    for (DBSyncHandler h : this.syncHandlers.values()) {
                        if (h.interest().equals(trimTableShardNum(table))) {
                            interested = true;
                            break;
                        }
                    }
                    if (!interested) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Ignoring un interest table : " + table);
                        }
                        continue;
                    }

                    Map<String, Object> maps = new HashMap<String, Object>();
                    if (action.equals(ActionType.INSERT) || action.equals(ActionType.UPDATE)) {
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();

                        Iterator<ArrayList<ColumnVal>> colValues = orc.getColumnValues().iterator();
                        while (colValues.hasNext()) {
                            ArrayList<ColumnVal> row = colValues.next();
                            for (int i = 0; i < row.size(); i++) {
                                String name = colSpecs.get(i).getName();
                                Object value = row.get(i).getValue();
                                maps.put(name, value);
                            }
                        }
                    } else if (action.equals(ActionType.DELETE)) {
                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc.getKeyValues();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc.getColumnValues();

                        for (int row = 0; row < columnValues.size() || row < keyValues.size(); row++) {
                            List<ColumnVal> keyValuesOfRow = keyValues.get(row);

                            for (int i = 0; i < keyValuesOfRow.size(); i++) {
                                String name = keySpecs.get(i).getName();
                                Object value = keyValuesOfRow.get(i).getValue();
                                maps.put(name, value);
                            }
                        }
                    } else {
                        logger.warn("Unrecognized action type: " + action);
                        return;
                    }
                    DataEvent dataEvent = new DataEvent(action, maps, schema, table, header, System.currentTimeMillis(), doCommit);
                    try {
                        waitingQueue.add(dataEvent);
                    } catch (Exception e) {
                        monitorLogger.error("Add to WaitingQueue Error:" + e.getMessage());
                    }
                }
            } else if (dbmsData instanceof LoadDataFileFragment) {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring load data file fragment");
            } else if (dbmsData instanceof RowIdData) {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring row ID data");
            } else {
                logger.warn("Unsupported DbmsData class: " + dbmsData.getClass().getName());
            }
        }

    }

    private static String trimTableShardNum(String table) {
        if (table == null) {
            return null;
        }
        int len = table.length();
        if (len <= 4) {
            return table;
        }
        int i = len - 1;
        for (; i >= 0; i--) {
            char c = table.charAt(i);
            if ((c < '0' || c > '9') && c != '_') {
                break;
            }
        }
        return table.substring(0, i + 1);
    }

    @Override
    public void commit() throws ReplicatorException, InterruptedException {
        // commit in the action thread. @see processing thread.
    }

    private void commit(ReplDBMSHeader latestHeader, int index) {
        if (latestHeader == null) {
            if (logger.isDebugEnabled())
                logger.debug("Unable to commit; last header is null");
            return;
        }
        try {
            Map<String, Object> prop = new HashMap<String, Object>();
            prop.put("task_id", taskId);
            prop.put("event_id", latestHeader.getEventId());
            prop.put("seqno", latestHeader.getSeqno());
            prop.put("last_frag", latestHeader.getLastFrag());
            prop.put("source_id", latestHeader.getSourceId());
            prop.put("fragno", latestHeader.getFragno());
            prop.put("extracted_timestamp", latestHeader.getExtractedTstamp());
            prop.put("epoch_number", latestHeader.getEpochNumber());

            if (logger.isDebugEnabled()) {
                logger.debug("Saving properties:" + prop);
            }

            String saveKey = makeKey(index);
            this.tedisManager.getHashCommands().putAll(configNamespace, saveKey, prop);
        } catch (Exception e) {
            logger.error("Saving last event error with data : " + latestHeader, e);
        }

    }

    private String makeKey(int index) {
        return configKey + "_" + serviceName + "_" + index;
    }

    @Override
    public ReplDBMSHeader getLastEvent() throws ReplicatorException, InterruptedException {
        ReplDBMSHeader last = null;
        for (int i = 0; i < threadSize; i++) {
            try {
                String savekey = makeKey(i);
                Map<String, Object> prop = this.tedisManager.getHashCommands().entries(configNamespace, savekey);
                if (logger.isDebugEnabled()) {
                    logger.debug("Getting properties:" + prop);
                }
                if (prop == null) {
                    continue;
                }
                String eventId = (String) prop.get("event_id");
                if (eventId == null) {
                    continue;
                }
                if (last == null || last.getEventId().compareToIgnoreCase(eventId) < 0) {
                    long seqno = (Long) prop.get("seqno");
                    boolean lastFrag = (Boolean) prop.get("last_frag");
                    String sourceId = (String) prop.get("source_id");
                    int fragno = (Integer) prop.get("fragno");
                    Timestamp extractedTstamp = (Timestamp) prop.get("extracted_timestamp");
                    long epochNumber = (Long) prop.get("epoch_number");
                    last = new ReplDBMSHeaderData(seqno, (short) fragno, lastFrag, sourceId, epochNumber, eventId, extractedTstamp);
                }
            } catch (Exception e) {
                logger.error("Get last event error>.", e);
            }
        }
        return last;
    }

    @Override
    public void rollback() throws InterruptedException {
        // Nothing to do.
    }

    @Override
    public void setTaskId(int id) {
        this.taskId = id;
    }

    @Override
    public void configure(PluginContext context) throws ReplicatorException, InterruptedException {
        this.serviceName = context.getServiceName();
        if (this.handlers == null) {
            throw new ReplicatorException("Handler can not be empty.");
        }
        try {
            this.syncHandlers = new HashMap<String, DBSyncHandler>();
            for (String handler : this.handlers.split(",")) {
                DBSyncHandler h = (DBSyncHandler) Class.forName(handler).newInstance();
                this.syncHandlers.put(h.interest(), h);
            }
        } catch (Exception e) {
            throw new ReplicatorException("Init handler failed.", e);
        }
        try {
            this.tedisManager = TedisManagerFactory.create(appName, version);
        } catch (Exception e) {
            throw new ReplicatorException("Redis init failed.", e);
        }

        for (int i = 0; i < threadSize; i++) {
            processingQueues.add(new ArrayBlockingQueue<DataEvent>(processingQueueLimit));
            Thread thread = new ProcessingThread(i);
            threadPool.add(thread);
            thread.start();
        }
        waitingThread = new WaitingThread();
        waitingThread.start();
    }

    @Override
    public void prepare(PluginContext context) throws ReplicatorException, InterruptedException {
        try {
            for (DBSyncHandler handler : this.syncHandlers.values()) {
                handler.init();
            }
        } catch (Exception e) {
            throw new ReplicatorException(e);
        }
    }

    @Override
    public void release(PluginContext context) throws ReplicatorException, InterruptedException {
        waitingThread.interrupt();
        for (Thread thread : threadPool) {
            thread.interrupt();
        }
        threadPool.clear();

        try {
            for (DBSyncHandler handler : this.syncHandlers.values()) {
                handler.release();
            }
        } catch (Exception e) {
            throw new ReplicatorException(e);
        }

    }

    private class WaitingThread extends Thread {

        @Override
        public void run() {
            Thread.currentThread().setName(serviceName + "-WaitingThread");
            while (true) {
                DataEvent dataEvent = null;
                try {
                    dataEvent = waitingQueue.take();
                    long wait = System.currentTimeMillis() - dataEvent.getTime();
                    if (wait >= 0 && wait < waitTime) {
                        Thread.sleep(waitTime - wait);
                    }
                    try {
                        processingQueues.get(dataEvent.getTableIndex() % threadSize).add(dataEvent);
                    } catch (Exception e) {
                        monitorLogger.error("Add to ProcessQueue Error:" + e.getMessage());
                    }
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    logger.error("Waiting error with data" + dataEvent, e);
                }
            }
        }

    }

    private class ProcessingThread extends Thread {

        private int id;

        public ProcessingThread(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            Thread.currentThread().setName(serviceName + "-ProcessingThread-" + id);
            while (true) {
                DataEvent dataEvent = null;
                try {
                    dataEvent = processingQueues.get(id).take();
                    DBSyncHandler handler = syncHandlers.get(trimTableShardNum(dataEvent.getTable()));
                    if (handler == null) {
                        logger.warn("Handler not found, somewhere has error.");
                        continue;
                    }

                    handler.process(dataEvent);

                    commit(dataEvent.getHeader(), id);
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    logger.error("Handle data error with event: ", e);
                }
            }
        }

    }

    public void setConfigNamespace(int configNamespace) {
        this.configNamespace = configNamespace;
    }

    public void setConfigKey(String configKey) {
        this.configKey = configKey;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setThreadSize(int threadSize) {
        this.threadSize = threadSize;
    }

    public void setWaitTime(long waitTime) {
        this.waitTime = waitTime;
    }

    public void setHandlers(String handlers) {
        this.handlers = handlers;
    }

}
