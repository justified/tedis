/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.pipeline;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.util.EventIdWatchPredicate;
import com.taobao.common.tedis.replicator.util.SeqnoWatchPredicate;
import com.taobao.common.tedis.replicator.util.SourceTimestampWatchPredicate;
import com.taobao.common.tedis.replicator.util.Watch;
import com.taobao.common.tedis.replicator.util.WatchAction;
import com.taobao.common.tedis.replicator.util.WatchManager;
import com.taobao.common.tedis.replicator.util.WatchPredicate;

public class StageProgressTracker {
    private static Logger logger = Logger.getLogger(StageProgressTracker.class);
    String name;

    private final int threadCount;
    private final TaskProgress[] taskInfo;

    private final WatchManager<ReplDBMSHeader> seqnoWatches = new WatchManager<ReplDBMSHeader>();
    private final WatchManager<ReplDBMSHeader> eventIdWatches = new WatchManager<ReplDBMSHeader>();
    private final WatchManager<ReplDBMSHeader> heartbeatWatches = new WatchManager<ReplDBMSHeader>();
    private final WatchManager<ReplDBMSHeader> timestampWatches = new WatchManager<ReplDBMSHeader>();

    private boolean shouldInterruptTask = false;

    WatchAction<ReplDBMSHeader> cancelAction = new WatchAction<ReplDBMSHeader>() {
        public void matched(ReplDBMSHeader event, int taskId) {
            taskInfo[taskId].setCancelled(true);
        }
    };

    private long eventCount = 0;
    private long loggingInterval = 0;
    private long applyLatencyMillis = 0;

    private long applySkipCount = 0;
    private SortedSet<Long> seqnosToBeSkipped = null;

    public StageProgressTracker(String name, int threadCount) {
        this.name = name;
        this.threadCount = threadCount;
        this.taskInfo = new TaskProgress[threadCount];

        for (int i = 0; i < taskInfo.length; i++)
            taskInfo[i] = new TaskProgress(name, i);

        if (logger.isDebugEnabled()) {
            logger.info("Initiating stage process tracker for stage: name=" + name + " threadCount=" + threadCount);
        }
    }

    public void setLoggingInterval(long loggingInterval) {
        this.loggingInterval = loggingInterval;
    }

    public void setApplySkipCount(long applySkipCount) {
        this.applySkipCount = applySkipCount;
    }

    public void setSeqnosToBeSkipped(SortedSet<Long> seqnosToBeSkipped) {
        this.seqnosToBeSkipped = seqnosToBeSkipped;
    }

    public synchronized ReplDBMSHeader getLastProcessedEvent(int taskId) {
        return taskInfo[taskId].getLastEvent();
    }

    public synchronized long getMinLastSeqno() {
        long minSeqno = Long.MAX_VALUE;
        for (TaskProgress progress : taskInfo) {
            ReplDBMSHeader event = progress.getLastEvent();
            if (event == null)
                minSeqno = -1;
            else
                minSeqno = Math.min(minSeqno, event.getSeqno());
        }
        return minSeqno;
    }

    public synchronized ReplDBMSHeader getMinLastEvent() {
        ReplDBMSHeader minEvent = null;
        for (TaskProgress progress : taskInfo) {
            ReplDBMSHeader event = progress.getLastEvent();
            if (event == null) {
                minEvent = null;
                break;
            } else if (minEvent == null || minEvent.getSeqno() > event.getSeqno()) {
                minEvent = event;
            }
        }
        return minEvent;
    }

    public synchronized long getApplyLatencyMillis() {
        // Latency may be sub-zero due to clock differences.
        if (applyLatencyMillis < 0)
            return 0;
        else
            return applyLatencyMillis;
    }

    public synchronized List<TaskProgress> cloneTaskProgress() {
        List<TaskProgress> progressList = new ArrayList<TaskProgress>();
        for (int i = 0; i < threadCount; i++)
            progressList.add(taskInfo[i].clone());
        return progressList;
    }

    public synchronized TaskProgress getTaskProgress(int taskId) {
        return taskInfo[taskId];
    }

    public synchronized void setLastProcessedEvent(int taskId, ReplDBMSHeader replEvent) throws InterruptedException {
        if (logger.isDebugEnabled()) {
            logger.debug("[" + name + "] setLastProcessedEvent: " + replEvent.getSeqno());
        }
        // Log global statistics.
        eventCount++;
        applyLatencyMillis = System.currentTimeMillis() - replEvent.getExtractedTstamp().getTime();

        // Log per-task statistics.
        taskInfo[taskId].incrementEventCount();
        taskInfo[taskId].setApplyLatencyMillis(applyLatencyMillis);

        // Log last processed event if greater than stored sequence number.
        if (taskInfo[taskId].getLastEvent() == null || taskInfo[taskId].getLastEvent().getSeqno() < replEvent.getSeqno()) {
            taskInfo[taskId].setLastEvent(replEvent);
        }

        // If we have a real event, process watches.
        if (replEvent instanceof ReplDBMSEvent) {
            seqnoWatches.process(replEvent, taskId);
            eventIdWatches.process(replEvent, taskId);
            heartbeatWatches.process(replEvent, taskId);
            timestampWatches.process(replEvent, taskId);
        }
        if (loggingInterval > 0 && eventCount % loggingInterval == 0)
            logger.info("Stage processing counter: event count=" + eventCount);
    }

    public synchronized void cancel(int taskId) {
        taskInfo[taskId].setCancelled(true);
    }

    public boolean isCancelled(int taskId) {
        return taskInfo[taskId].isCancelled();
    }

    public synchronized void cancelAll() {
        for (TaskProgress progress : taskInfo)
            progress.setCancelled(true);
    }

    public boolean allCancelled() {
        for (TaskProgress progress : taskInfo) {
            if (progress.isCancelled())
                return false;
        }
        return true;
    }

    public boolean shouldInterruptTask() {
        return shouldInterruptTask;
    }

    public synchronized void release() {
        this.eventIdWatches.cancelAll();
        this.seqnoWatches.cancelAll();
    }

    public synchronized Future<ReplDBMSHeader> watchForProcessedSequenceNumber(long seqno, boolean cancel) throws InterruptedException {
        SeqnoWatchPredicate seqnoPredicate = new SeqnoWatchPredicate(seqno);
        return waitForEvent(seqnoPredicate, seqnoWatches, cancel);
    }

    public synchronized Future<ReplDBMSHeader> watchForProcessedEventId(String eventId, boolean cancel) throws InterruptedException {
        EventIdWatchPredicate eventPredicate = new EventIdWatchPredicate(eventId);
        return waitForEvent(eventPredicate, eventIdWatches, cancel);
    }

    public synchronized Future<ReplDBMSHeader> watchForProcessedTimestamp(Timestamp timestamp, boolean cancel) throws InterruptedException {
        SourceTimestampWatchPredicate predicate = new SourceTimestampWatchPredicate(timestamp);
        return waitForEvent(predicate, timestampWatches, cancel);
    }

    private Future<ReplDBMSHeader> waitForEvent(WatchPredicate<ReplDBMSHeader> predicate, WatchManager<ReplDBMSHeader> manager, boolean cancel) throws InterruptedException {
        // Find the trailing event that has been processed across all tasks.
        ReplDBMSHeader lastEvent = getMinLastEvent();
        Watch<ReplDBMSHeader> watch;
        if (lastEvent == null || !predicate.match(lastEvent)) {
            // We have not reached the requested event, so we have to enqueue a watch.
            if (cancel)
                watch = manager.watch(predicate, threadCount, cancelAction);
            else
                watch = manager.watch(predicate, threadCount);
            offerAll(watch);
        } else {
            // We have already reached it, so signal that we are cancelled, post
            // an interrupt flag, and return the current event.
            watch = new Watch<ReplDBMSHeader>(predicate, threadCount);
            offerAll(watch);
            if (cancel) {
                cancelAll();
                shouldInterruptTask = true;
            }
        }

        return watch;
    }

    private void offerAll(Watch<ReplDBMSHeader> watch) throws InterruptedException {
        for (int i = 0; i < this.taskInfo.length; i++) {
            ReplDBMSHeader event = taskInfo[i].getLastEvent();
            if (event != null)
                watch.offer(event, i);
        }
    }

    public synchronized boolean skip(ReplDBMSEvent event) {
        // If we are skipping the first N transactions to be applied,
        // try again.
        if (this.applySkipCount > 0) {
            logger.info("Skipping event: seqno=" + event.getSeqno() + " fragno=" + event.getFragno(), null);
            if (event.getLastFrag())
                applySkipCount--;
            return true;
        } else if (this.seqnosToBeSkipped != null) {
            // Purge skip numbers processing has already reached.
            long minSeqno = getMinLastSeqno();
            while (!this.seqnosToBeSkipped.isEmpty() && this.seqnosToBeSkipped.first() < minSeqno)
                this.seqnosToBeSkipped.remove(this.seqnosToBeSkipped.first());

            if (!this.seqnosToBeSkipped.isEmpty()) {
                // If we are in the skip list, then skip!
                if (seqnosToBeSkipped.contains(event.getSeqno())) {
                    if (logger.isDebugEnabled())
                        logger.debug("Skipping event with seqno " + event.getSeqno());
                    // Skip event and remove seqno after last fragment.
                    if (event.getLastFrag())
                        this.seqnosToBeSkipped.remove(event.getSeqno());
                    return true;
                }
                // else seqnosToBeSkipped.first() > event.getSeqno()
                // so let's process this event
            } else {
                // the list is now empty... just free the list
                this.seqnosToBeSkipped = null;
                if (logger.isDebugEnabled())
                    logger.debug("No more events to be skipped");
            }
        }

        // No match, so we will process the event.
        return false;
    }
}