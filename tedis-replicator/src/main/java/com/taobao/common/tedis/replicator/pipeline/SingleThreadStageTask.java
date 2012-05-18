/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.pipeline;

import java.util.List;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ErrorNotification;
import com.taobao.common.tedis.replicator.InSequenceNotification;
import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.applier.Applier;
import com.taobao.common.tedis.replicator.applier.ApplierException;
import com.taobao.common.tedis.replicator.conf.FailurePolicy;
import com.taobao.common.tedis.replicator.event.ReplControlEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.event.ReplEvent;
import com.taobao.common.tedis.replicator.event.ReplOptionParams;
import com.taobao.common.tedis.replicator.extractor.Extractor;
import com.taobao.common.tedis.replicator.extractor.ExtractorException;
import com.taobao.common.tedis.replicator.filter.Filter;
import com.taobao.common.tedis.replicator.plugin.PluginContext;
import com.taobao.common.tedis.replicator.statemachine.event.EventDispatcher;

public class SingleThreadStageTask implements Runnable {
    private static Logger logger = Logger.getLogger(SingleThreadStageTask.class);
    private Stage stage;
    private int taskId;
    private Extractor extractor;
    private List<Filter> filters;
    private Applier applier;
    private boolean usingBlockCommit;
    private int blockCommitRowsCount;
    private EventDispatcher eventDispatcher;
    private Schedule schedule;
    private String name;

    private long blockEventCount = 0;
    private TaskProgress taskProgress;

    private volatile boolean cancelled = false;

    public SingleThreadStageTask(Stage stage, int taskId) {
        this.taskId = taskId;
        this.name = stage.getName() + "-" + taskId;
        this.stage = stage;
        this.blockCommitRowsCount = stage.getBlockCommitRowCount();
        this.usingBlockCommit = (blockCommitRowsCount > 1);
        this.taskProgress = stage.getProgressTracker().getTaskProgress(taskId);
    }

    public int getTaskId() {
        return taskId;
    }

    public void setEventDispatcher(EventDispatcher eventDispatcher) {
        this.eventDispatcher = eventDispatcher;
    }

    public void setSchedule(Schedule schedule) {
        this.schedule = schedule;
    }

    public void setExtractor(Extractor extractor) {
        this.extractor = extractor;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
    }

    public void setApplier(Applier applier) {
        this.applier = applier;
    }

    public Extractor getExtractor() {
        return extractor;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public Applier getApplier() {
        return applier;
    }

    public String getName() {
        return name;
    }

    public void cancel() {
        cancelled = true;
    }

    public void run() {
        logInfo("Starting stage task thread", null);
        taskProgress.begin();

        runTask();

        logInfo("Terminating processing for stage task thread", null);
        ReplDBMSHeader lastEvent = stage.getProgressTracker().getLastProcessedEvent(taskId);
        if (lastEvent != null) {
            String msg = "Last successfully processed event prior to termination: seqno=" + lastEvent.getSeqno() + " eventid=" + lastEvent.getEventId();
            logInfo(msg, null);
        }
        logInfo("Task event count: " + taskProgress.getEventCount(), null);
        schedule.taskEnd();
    }

    public void runTask() {
        PluginContext context = stage.getPluginContext();

        ReplDBMSEvent currentEvent = null;
        ReplEvent genericEvent = null;
        ReplDBMSEvent event = null;

        String currentService = null;

        try {
            // If we are supposed to auto-synchronize, do it now.
            if (stage.isAutoSync()) {
                // Indicate that we are ready to go.
                eventDispatcher.put(new InSequenceNotification());
            }

            while (!cancelled) {
                // If we have a pending currentEvent from the last iteration,
                // we should log it now, then test to see whether the task has
                // been cancelled.
                if (currentEvent != null) {
                    schedule.setLastProcessedEvent(currentEvent);
                    currentEvent = null;
                }

                // Check for cancellation and exit loop if it has occurred.
                if (schedule.isCancelled()) {
                    logInfo("Task has been cancelled", null);
                    break;
                }

                // Fetch the next event.
                event = null;
                try {
                    taskProgress.beginExtractInterval();
                    genericEvent = extractor.extract();
                } catch (ExtractorException e) {
                    String message = "Event extraction failed";
                    if (context.getExtractorFailurePolicy() == FailurePolicy.STOP) {
                        if (logger.isDebugEnabled())
                            logger.debug(message, e);
                        eventDispatcher.put(new ErrorNotification(message, e));
                        break;
                    } else {
                        logError(message, e);
                        continue;
                    }
                } finally {
                    taskProgress.endExtractInterval();
                }

                if (genericEvent == null) {
                    if (logger.isDebugEnabled())
                        logger.debug("No event extracted, retrying...");
                    currentEvent = null;
                    continue;
                }

                if (usingBlockCommit && genericEvent instanceof ReplDBMSEvent) {
                    ReplDBMSEvent re = (ReplDBMSEvent) genericEvent;
                    String newService = re.getDBMSEvent().getMetadataOptionValue(ReplOptionParams.SERVICE);
                    if (currentService == null)
                        currentService = newService;
                    else if (!currentService.equals(newService)) {
                        if (re.getFragno() == 0) {
                            if (logger.isDebugEnabled()) {
                                String msg = String.format("Committing due to service change: prev svc=%s seqno=%d new_svc=%s\n", currentService, re.getSeqno(), newService);
                                logger.debug(msg);
                            }
                            applier.commit();
                            blockEventCount = 0;
                            taskProgress.incrementBlockCount();
                        } else {
                            String msg = String.format("Service name change between fragments: prev svc=%s seqno=%d fragno=%d new_svc=%s\n", currentService, re.getSeqno(), re.getFragno(), newService);
                            logger.warn(msg);
                        }
                    }
                }

                int disposition = schedule.advise(genericEvent);
                if (disposition == Schedule.PROCEED) {
                    // Go ahead and apply this event.
                } else if (disposition == Schedule.CONTINUE_NEXT) {
                    updatePosition(genericEvent, false);
                    currentEvent = null;
                    continue;
                } else if (disposition == Schedule.CONTINUE_NEXT_COMMIT) {
                    updatePosition(genericEvent, true);
                    currentEvent = null;
                    continue;
                } else if (disposition == Schedule.QUIT) {
                    if (logger.isDebugEnabled())
                        logger.debug("Quitting task processing loop");
                    updatePosition(genericEvent, false);
                    break;
                } else {
                    // This is a serious bug.
                    throw new ReplicatorException("Unexpected schedule disposition on event: disposition=" + disposition + " event=" + genericEvent.toString());
                }

                // Convert to a proper log event and proceed.
                event = (ReplDBMSEvent) genericEvent;
                if (logger.isDebugEnabled()) {
                    logger.debug("Extracted event: seqno=" + event.getSeqno() + " fragno=" + event.getFragno());
                }
                currentEvent = event;

                // Run filters.
                taskProgress.beginFilterInterval();
                for (Filter f : filters) {
                    if ((event = f.filter(event)) == null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Event discarded by filter: name=" + f.getClass().toString());
                        }
                        break;
                    }
                }
                taskProgress.endFilterInterval();

                if (event == null) {
                    // The event was filtered, try next.
                    if (logger.isDebugEnabled())
                        logger.debug("The event was filtered, retrying...");
                    continue;
                }

                boolean doRollback = false;
                boolean unsafeForBlockCommit = event.getDBMSEvent().getMetadataOptionValue(ReplOptionParams.UNSAFE_FOR_BLOCK_COMMIT) != null;

                if (event.getFragno() == 0 && !event.getLastFrag()) {
                    applier.commit();
                    blockEventCount = 0;
                    taskProgress.incrementBlockCount();
                } else {
                    boolean isRollback = event.getDBMSEvent().getMetadataOptionValue(ReplOptionParams.ROLLBACK) != null;
                    if (event.getFragno() == 0 && isRollback) {
                        applier.commit();
                        blockEventCount = 0;
                        taskProgress.incrementBlockCount();
                        doRollback = true;
                    } else if (unsafeForBlockCommit) {
                        applier.commit();
                        blockEventCount = 0;
                        taskProgress.incrementBlockCount();
                    }

                }

                boolean doCommit = false;

                if (unsafeForBlockCommit) {
                    doCommit = true;
                } else if (usingBlockCommit) {
                    blockEventCount++;
                    if (event.getLastFrag() && ((blockEventCount >= blockCommitRowsCount) || !extractor.hasMoreEvents())) {
                        doCommit = true;
                    }
                } else {
                    doCommit = event.getLastFrag();
                }

                try {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Applying event: seqno=" + event.getSeqno() + " fragno=" + event.getFragno() + " doCommit=" + doCommit);
                    }
                    taskProgress.beginApplyInterval();
                    applier.apply(event, doCommit, doRollback);
                    if (doCommit) {
                        blockEventCount = 0;
                        taskProgress.incrementBlockCount();
                    }
                } catch (ApplierException e) {
                    if (context.getApplierFailurePolicy() == FailurePolicy.STOP) {
                        throw e;
                    } else {
                        String message = "Event application failed: seqno=" + event.getSeqno() + " fragno=" + event.getFragno() + " message=" + e.getMessage();
                        logError(message, e);
                        continue;
                    }
                } finally {
                    taskProgress.endApplyInterval();
                }
            }

            applier.commit();
        } catch (InterruptedException e) {
            if (!schedule.isCancelled())
                logger.warn("Received unexpected interrupt in stage task: " + stage.getName());
            else if (logger.isDebugEnabled())
                logger.debug("Task loop interrupted", e);

            try {
                applier.rollback();
            } catch (InterruptedException e1) {
                logWarn("Task cancelled while trying to rollback following cancellation", null);
            }
        } catch (ApplierException e) {
            String message = "Event application failed: seqno=" + event.getSeqno() + " fragno=" + event.getFragno() + " message=" + e.getMessage();
            logError(message, e);
            dispatchErrorEvent(new ErrorNotification(message, event.getSeqno(), event.getEventId(), e));
        } catch (Throwable e) {
            String msg = "Stage task failed: " + stage.getName();
            if (event == null) {
                dispatchErrorEvent(new ErrorNotification(msg, e));
            } else {
                dispatchErrorEvent(new ErrorNotification(msg, event.getSeqno(), event.getEventId(), e));
            }
            logger.info("Unexpected error: " + msg, e);
        }
    }

    private void updatePosition(ReplEvent replEvent, boolean doCommit) throws ReplicatorException, InterruptedException {
        ReplDBMSHeader header = null;
        if (replEvent instanceof ReplControlEvent) {
            ReplControlEvent controlEvent = (ReplControlEvent) replEvent;
            header = controlEvent.getHeader();
        } else if (replEvent instanceof ReplDBMSEvent) {
            header = (ReplDBMSEvent) replEvent;
        }

        if (header == null) {
            if (logger.isDebugEnabled())
                logger.debug("Unable to update position due to null event value");
            return;
        }

        if (usingBlockCommit) {
            blockEventCount++;
            if ((blockEventCount >= blockCommitRowsCount) || !extractor.hasMoreEvents()) {
                doCommit = true;
            } else {
                doCommit |= false;
            }
        } else
            doCommit = true;

        if (logger.isDebugEnabled()) {
            logger.debug("Updating position: seqno=" + header.getSeqno() + " doCommit=" + doCommit);
        }
        taskProgress.beginApplyInterval();
        applier.updatePosition(header, doCommit);
        taskProgress.endApplyInterval();
        if (doCommit) {
            blockEventCount = 0;
            taskProgress.incrementBlockCount();
        }
    }

    private void dispatchErrorEvent(ErrorNotification en) {
        try {
            eventDispatcher.put(en);
        } catch (InterruptedException e) {
            logWarn("Task cancelled while posting error notification", null);
        }
    }

    private void logInfo(String message, Throwable e) {
        if (e == null)
            logger.info(message);
        else
            logger.info(message, e);
    }

    private void logWarn(String message, Throwable e) {
        if (e == null)
            logger.warn(message);
        else
            logger.warn(message, e);
    }

    private void logError(String message, Throwable e) {
        if (e == null)
            logger.error(message);
        else
            logger.error(message, e);
    }
}