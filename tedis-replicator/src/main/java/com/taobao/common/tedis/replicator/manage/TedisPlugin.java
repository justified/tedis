/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.manage;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.ReplicatorProperties;
import com.taobao.common.tedis.replicator.conf.ReplicatorRuntime;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.extractor.Extractor;
import com.taobao.common.tedis.replicator.extractor.ExtractorException;
import com.taobao.common.tedis.replicator.manage.events.GoOfflineEvent;
import com.taobao.common.tedis.replicator.manage.events.OfflineNotification;
import com.taobao.common.tedis.replicator.pipeline.Pipeline;
import com.taobao.common.tedis.replicator.pipeline.TaskProgress;
import com.taobao.common.tedis.replicator.storage.Store;

public class TedisPlugin implements OpenReplicatorPlugin {
    private static Logger logger = Logger.getLogger(TedisPlugin.class);

    // Configuration is stored in the ReplicatorRuntime
    private ReplicatorProperties properties = null;
    private ReplicatorRuntime runtime;
    private Pipeline pipeline;
    private ReplicatorContext context;

    public void prepare(ReplicatorContext context) throws ReplicatorException {
        this.context = context;
    }

    public void release() throws ReplicatorException {
        try {
            doShutdown(new ReplicatorProperties());
            properties = null;
        } catch (Throwable e) {
            logger.error("Replicator service shutdown failed due to underlying error: ", e);
            throw new ReplicatorException("Replicator service shutdown failed due to underlying error: " + e);
        }
    }

    public void configure(ReplicatorProperties properties) throws ReplicatorException {
        this.properties = properties;

        // Release existing runtime, if any, and generate a new one.
        doCreateRuntime();
    }

    /**
     * Puts the replicator into the online state, which turns on replication.
     */
    public void online(ReplicatorProperties params) throws Exception {
        // Release existing runtime, if any, and generate a new one.
        doCreateRuntime();

        // Start replication pipeline.
        try {
            runtime.prepare();
            pipeline = runtime.getPipeline();

            // Set initial event ID if specified.
            String initialEventId = params.getString(ReplicatorParams.INIT_EVENT_ID);
            if (initialEventId != null) {
                logger.info("Initializing extractor to start at specific event ID: " + initialEventId);
                pipeline.setInitialEventId(initialEventId);
            }

            // Set apply skip events, if specified and higher than 0.
            if (params.getString(ReplicatorParams.SKIP_APPLY_EVENTS) != null) {
                try {
                    long skipCount = params.getLong(ReplicatorParams.SKIP_APPLY_EVENTS);
                    if (skipCount < 0)
                        throw new ReplicatorException("Apply skip count may not be less than 0: " + skipCount);
                    else
                        pipeline.setApplySkipCount(skipCount);
                } catch (NumberFormatException e) {
                    throw new ReplicatorException("Invalid apply skip count: " + params.getString(ReplicatorParams.SKIP_APPLY_EVENTS));
                }
            }

            // Set apply skip seqnos.
            if (params.getString(ReplicatorParams.SKIP_APPLY_SEQNOS) != null) {
                try {
                    String seqnosToBeSkipped = params.getString(ReplicatorParams.SKIP_APPLY_SEQNOS);
                    SortedSet<Long> seqnos = new TreeSet<Long>();

                    String[] seqnoRanges = seqnosToBeSkipped.split(",");
                    for (String seqnoRange : seqnoRanges) {
                        String[] seqnoBoundaries = seqnoRange.trim().split("-");
                        if (seqnoBoundaries.length == 1) {
                            seqnos.add(Long.parseLong(seqnoBoundaries[0].trim()));
                        } else if (seqnoBoundaries.length == 2) {
                            Long start = Long.parseLong(seqnoBoundaries[0].trim());
                            Long end = Long.parseLong(seqnoBoundaries[1].trim());
                            if (start < end) {
                                for (Long i = start; i <= end; i++) {
                                    seqnos.add(i);
                                }
                            } else {
                                throw new ReplicatorException("Invalid apply skip seqnos: " + params.getString(ReplicatorParams.SKIP_APPLY_SEQNOS));
                            }
                        } else {
                            throw new ReplicatorException("Invalid apply skip seqnos: " + params.getString(ReplicatorParams.SKIP_APPLY_SEQNOS));
                        }
                    }
                    logger.info("Going online and skipping events " + seqnos);
                    pipeline.setApplySkipEvents(seqnos);
                } catch (NumberFormatException e) {
                    throw new ReplicatorException("Invalid apply skip seqnos: " + params.getString(ReplicatorParams.SKIP_APPLY_SEQNOS));
                }
            }

            // Stay online to a specified sequence number.
            if (params.get(ReplicatorParams.ONLINE_TO_SEQNO) != null) {
                long seqno = params.getLong(ReplicatorParams.ONLINE_TO_SEQNO);
                logger.info("Initializing pipeline to go offline after processing seqno: " + seqno);
                pipeline.shutdownAfterSequenceNumber(seqno);
            }

            // Stay online to a specified event ID.
            if (params.get(ReplicatorParams.ONLINE_TO_EVENT_ID) != null) {
                String eventId = params.getString(ReplicatorParams.ONLINE_TO_EVENT_ID);
                logger.info("Initializing pipeline to go offline after processing event ID: " + eventId);
                pipeline.shutdownAfterEventId(eventId);
            }

            // Stay online to a specified timestamp.
            if (params.get(ReplicatorParams.ONLINE_TO_TIMESTAMP) != null) {
                long timeMillis = params.getLong(ReplicatorParams.ONLINE_TO_TIMESTAMP);
                DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date toDate = new Date(timeMillis);
                Timestamp ts = new Timestamp(timeMillis);
                logger.info("Scheduling pipeline to go offline after processing source timestamp: " + formatter.format(toDate));
                pipeline.shutdownAfterTimestamp(ts);
            }

            // Start the pipeline.
            pipeline.start(context.getEventDispatcher());
        } catch (ReplicatorException e) {
            throw e;
        } catch (Throwable e) {
            String pendingError = "Unable to start replication service due to underlying error";
            logger.error(pendingError, e);
            throw new ReplicatorException(pendingError + ": " + e);
        }
    }

    /**
     * Puts the replicator immediately into the offline state, which turns off
     * replication. This operation is a hard shutdown that does no clean-up. If
     * clean-up is required, call deferredShutdown() instead.
     */
    public void offline(ReplicatorProperties params) throws Exception {
        try {
            // Now we go ahead and shutdown completely.
            doShutdown(params);
            context.getEventDispatcher().put(new OfflineNotification());
        } catch (ReplicatorException e) {
            String pendingError = "Replicator service shutdown failed";
            if (logger.isDebugEnabled())
                logger.debug(pendingError, e);
            throw e;
        } catch (Throwable e) {
            String pendingError = "Replicator service shutdown failed due to underlying error";
            logger.error(pendingError, e);
            throw new ReplicatorException(pendingError + e);
        }
    }

    public void offlineDeferred(ReplicatorProperties params) throws Exception {
        try {
            if (params.get(ReplicatorParams.OFFLINE_TRANSACTIONAL) != null) {
                // Shut down processing in orderly fashion and signal that we
                // are down.
                logger.info("Initiating clean shutdown at next transaction");
                pipeline.shutdown(false);
                pipeline.getContext().getEventDispatcher().put(new GoOfflineEvent());
            } else if (params.get(ReplicatorParams.OFFLINE_AT_SEQNO) != null) {
                // Shut down processing at a particular sequence number.
                long seqno = params.getLong(ReplicatorParams.OFFLINE_AT_SEQNO);
                logger.info("Initializing pipeline to go offline after processing seqno: " + seqno);
                pipeline.shutdownAfterSequenceNumber(seqno);
            } else if (params.get(ReplicatorParams.OFFLINE_AT_EVENT_ID) != null) {
                // Shut down processing at a particular event ID.
                String eventId = params.getString(ReplicatorParams.OFFLINE_AT_EVENT_ID);
                logger.info("Initializing pipeline to go offline after processing event ID: " + eventId);
                pipeline.shutdownAfterEventId(eventId);
            }

            // Stay online to a specified timestamp.
            else if (params.get(ReplicatorParams.OFFLINE_AT_TIMESTAMP) != null) {
                long timeMillis = params.getLong(ReplicatorParams.OFFLINE_AT_TIMESTAMP);
                DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date toDate = new Date(timeMillis);
                Timestamp ts = new Timestamp(timeMillis);
                logger.info("Scheduling pipeline to go offline after processing source timestamp: " + formatter.format(toDate));
                pipeline.shutdownAfterTimestamp(ts);
            }

            // If there is no parameter provided, just enqueue an event to
            // perform a hard shutdown.
            else {
                logger.info("Initiating immediate pipeline shutdown");
                context.getEventDispatcher().put(new GoOfflineEvent());
            }
        } catch (ReplicatorException e) {
            String pendingError = "Replicator deferred service shutdown failed";
            if (logger.isDebugEnabled())
                logger.debug(pendingError, e);
            throw e;
        } catch (Throwable e) {
            String pendingError = "Replicator deferred service shutdown failed due to underlying error";
            logger.error(pendingError, e);
            throw new ReplicatorException(pendingError + e);
        }
    }

    /**
     * Implements a flush operation to synchronize the state of the database
     * with the replication log and return a comparable event ID that can be
     * used in a wait operation on a slave.
     *
     * @param timeout
     *            Number of seconds to wait. 0 is indefinite.
     * @return The event ID at which the log is synchronized
     */
    public String flush(long timeout) throws Exception {
        // Wait for the event we were seeking to show up.
        Future<ReplDBMSHeader> expectedEvent = runtime.getPipeline().flush();
        ReplDBMSHeader event = null;
        try {
            if (timeout <= 0)
                event = expectedEvent.get();
            else
                event = expectedEvent.get(timeout, TimeUnit.SECONDS);
        } finally {
            expectedEvent.cancel(false);
        }

        long seqno = event.getSeqno();
        logger.info("SyncEvent-Flush: Flush complete.  Returning sequence number: " + seqno);
        return new Long(seqno).toString();
    }

    public boolean waitForAppliedEvent(String event, long timeout) throws Exception {
        // The event returns a Future on the THLEvent we are
        // expecting. We just wait for it.
        long seqno = new Long(event);

        if (pipeline == null)
            throw new ReplicatorException("Invalid replicator state for this operation. Cannot wait for event " + seqno + " to be applied.");

        Future<ReplDBMSHeader> expectedEvent = pipeline.watchForAppliedSequenceNumber(seqno);

        ReplDBMSHeader replEvent = null;
        try {
            if (timeout <= 0)
                replEvent = expectedEvent.get();
            else
                replEvent = expectedEvent.get(timeout, TimeUnit.SECONDS);
            logger.info("SyncEvent-WaitSeqno: Sequence number " + seqno + " found or surpassed with sequence number: " + replEvent.getSeqno());
        } catch (TimeoutException e) {
            return false;
        } finally {
            expectedEvent.cancel(true);
        }
        // If we got here it worked.
        return true;
    }

    /**
     * Returns the current replicator status as a set of name-value pairs.
     */
    public HashMap<String, String> status() throws Exception {
        ReplicatorProperties statusProps = new ReplicatorProperties();

        // Set generic values that are always identical.
        statusProps.setString(Replicator.SEQNO_TYPE, Replicator.SEQNO_TYPE_LONG);

        // Set default values.
        statusProps.setLong(Replicator.APPLIED_LAST_SEQNO, -1);
        statusProps.setString(Replicator.APPLIED_LAST_EVENT_ID, "NONE");
        statusProps.setLong(Replicator.LATEST_EPOCH_NUMBER, -1);
        statusProps.setDouble(Replicator.APPLIED_LATENCY, -1.0);
        statusProps.setString(Replicator.CURRENT_EVENT_ID, "NONE");
        statusProps.setString(Replicator.OFFLINE_REQUESTS, "NONE");

        // The following logic avoids race conditions that may cause
        // different sources of information to be null.
        Pipeline pipeline = null;
        List<String> extensions = null;
        if (runtime != null) {
            pipeline = runtime.getPipeline();
            extensions = runtime.getExtensionNames();
        }
        ReplDBMSHeader lastEvent = null;
        if (pipeline != null) {
            lastEvent = pipeline.getLastAppliedEvent();

            // The current event ID may be null for slaves or non-DBMS sources.
            Extractor headExtractor = pipeline.getHeadExtractor();
            String currentEventId = null;
            if (headExtractor != null) {
                try {
                    currentEventId = headExtractor.getCurrentResourceEventId();
                    if (currentEventId != null)
                        statusProps.setString(Replicator.CURRENT_EVENT_ID, currentEventId);
                } catch (ExtractorException e) {
                    statusProps.setString(Replicator.CURRENT_EVENT_ID, "ERROR");
                    if (logger.isDebugEnabled())
                        logger.debug("Unable to get current resource event ID", e);
                }
            }

            // Get the current list of offline requests.
            String offlineRequests = pipeline.getOfflineRequests();
            if (offlineRequests.length() > 0)
                statusProps.setString(Replicator.OFFLINE_REQUESTS, offlineRequests);

            // Show event processing information.
            if (lastEvent != null) {
                statusProps.setLong(Replicator.APPLIED_LAST_SEQNO, lastEvent.getSeqno());
                statusProps.setLong(Replicator.LATEST_EPOCH_NUMBER, lastEvent.getEpochNumber());
                statusProps.setString(Replicator.APPLIED_LAST_EVENT_ID, lastEvent.getEventId());
                statusProps.setDouble(Replicator.APPLIED_LATENCY, pipeline.getApplyLatency());
            }
        }

        // Fill out non-pipeline data.
        if (extensions == null)
            statusProps.setString(Replicator.EXTENSIONS, "");
        else
            statusProps.setStringList(Replicator.EXTENSIONS, extensions);

        return statusProps.hashMap();
    }

    public List<Map<String, String>> statusList(String name) throws Exception {
        List<Map<String, String>> statusList = new ArrayList<Map<String, String>>();

        // Fetch the pipeline.
        Pipeline pipeline = null;
        if (runtime != null)
            pipeline = runtime.getPipeline();

        // If we have a pipeline, process the status request.
        if (pipeline != null) {
            if ("tasks".equals(name)) {
                // Fetch task information and put into the list.
                List<TaskProgress> progressList = pipeline.getTaskProgress();
                for (TaskProgress progress : progressList) {
                    Map<String, String> props = new HashMap<String, String>();
                    props.put("stage", progress.getStageName());
                    props.put("taskId", Integer.toString(progress.getTaskId()));
                    props.put("cancelled", Boolean.toString(progress.isCancelled()));
                    props.put("eventCount", Long.toString(progress.getEventCount()));
                    long blockCount = progress.getBlockCount();
                    double avgBlock;
                    if (blockCount > 0)
                        avgBlock = (double) progress.getEventCount() / blockCount;
                    else
                        avgBlock = 0.0;
                    props.put("averageBlockSize", String.format("%-10.3f", avgBlock));
                    props.put("appliedLatency", Double.toString(progress.getApplyLatencySeconds()));
                    props.put("extractTime", Double.toString(progress.getTotalExtractSeconds()));
                    props.put("filterTime", Double.toString(progress.getTotalFilterSeconds()));
                    props.put("applyTime", Double.toString(progress.getTotalApplySeconds()));
                    props.put("otherTime", Double.toString(progress.getTotalOtherSeconds()));
                    props.put("state", progress.getState().toString());
                    ReplDBMSHeader lastEvent = progress.getLastEvent();
                    if (lastEvent == null) {
                        props.put("appliedLastSeqno", "-1");
                        props.put("appliedLastEventId", "");
                    } else {
                        props.put("appliedLastSeqno", Long.toString(lastEvent.getSeqno()));
                        props.put("appliedLastEventId", lastEvent.getEventId());
                    }
                    statusList.add(props);
                }
            } else if ("stores".equals(name)) {
                // Fetch task information and put into the list.
                for (String storeName : pipeline.getStoreNames()) {
                    Store store = pipeline.getStore(storeName);
                    ReplicatorProperties storeProps = store.status();
                    storeProps.setString("name", storeName);
                    storeProps.setString("storeClass", store.getClass().getName());
                    statusList.add(storeProps.hashMap());
                }
            } else
                throw new ReplicatorException("Unrecognized status list type: " + name);
        }

        // Return whatever we found.
        return statusList;
    }

    // Shut down current pipeline, if any.
    private void doShutdown(ReplicatorProperties params) throws ReplicatorException, InterruptedException {
        if (pipeline != null) {
            pipeline.shutdown(true);
            pipeline = null;
        }
        if (runtime != null) {
            runtime.release();
            runtime = null;
        }
    }

    // Release existing runtime, if any, and generate a new one.
    private void doCreateRuntime() throws ReplicatorException {
        if (runtime != null) {
            runtime.release();
        }
        runtime = new ReplicatorRuntime(properties, context);
        runtime.configure();
    }

    public ReplicatorRuntime getReplicatorRuntime() {
        return runtime;
    }
}