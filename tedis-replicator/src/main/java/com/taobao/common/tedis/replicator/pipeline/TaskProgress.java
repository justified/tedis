/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.pipeline;

import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;

public class TaskProgress {
    private final String stageName;
    private final int taskId;
    private ReplDBMSHeader lastEvent = null;
    private boolean cancelled = false;
    private long eventCount = 0;
    private long blockCount = 0;
    private long applyLatencyMillis = 0;
    private long startMillis;
    private long totalExtractMillis = 0;
    private long totalFilterMillis = 0;
    private long totalApplyMillis = 0;
    private TaskState state = TaskState.other;

    private long intervalStartMillis = 0;

    private long endMillis = 0;

    TaskProgress(String stageName, int taskId) {
        this.stageName = stageName;
        this.taskId = taskId;
    }

    public TaskProgress(TaskProgress other) {
        this.stageName = other.getStageName();
        this.taskId = other.getTaskId();
        this.applyLatencyMillis = other.getApplyLatencyMillis();
        this.cancelled = other.isCancelled();
        this.eventCount = other.getEventCount();
        this.blockCount = other.getBlockCount();
        this.lastEvent = other.getLastEvent();
        this.startMillis = other.getStartMillis();
        this.endMillis = other.getEndMillis();
        this.totalApplyMillis = other.getTotalApplyMillis();
        this.totalExtractMillis = other.getTotalExtractMillis();
        this.totalFilterMillis = other.getTotalFilterMillis();
        this.state = other.getState();
    }

    public void begin() {
        startMillis = System.currentTimeMillis();
        endMillis = startMillis;
    }

    public String getStageName() {
        return this.stageName;
    }

    public int getTaskId() {
        return this.taskId;
    }

    public ReplDBMSHeader getLastEvent() {
        return lastEvent;
    }

    public void setLastEvent(ReplDBMSHeader lastEvent) {
        this.lastEvent = lastEvent;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    public long getEventCount() {
        return eventCount;
    }

    public void setEventCount(long eventCount) {
        this.eventCount = eventCount;
    }

    public void incrementEventCount() {
        this.eventCount++;
    }

    public long getBlockCount() {
        return blockCount;
    }

    public void setBlockCount(long blockCount) {
        this.blockCount = blockCount;
    }

    public void incrementBlockCount() {
        this.blockCount++;
    }

    public long getApplyLatencyMillis() {
        if (applyLatencyMillis < 0)
            return 0;
        else
            return applyLatencyMillis;
    }

    public double getApplyLatencySeconds() {
        long applyLatencyMillis = getApplyLatencyMillis();
        return applyLatencyMillis / 1000.0;
    }

    public void setApplyLatencyMillis(long applyLatencyMillis) {
        this.applyLatencyMillis = applyLatencyMillis;
    }

    public long getStartMillis() {
        return startMillis;
    }

    public long getEndMillis() {
        return endMillis;
    }

    public long getTotalExtractMillis() {
        return totalExtractMillis;
    }

    public double getTotalExtractSeconds() {
        return getTotalExtractMillis() / 1000.0;
    }

    public void beginExtractInterval() {
        intervalStartMillis = System.currentTimeMillis();
        endMillis = intervalStartMillis;
        state = TaskState.extract;
    }

    public void endExtractInterval() {
        endMillis = System.currentTimeMillis();
        totalExtractMillis += (endMillis - intervalStartMillis);
        state = TaskState.other;
    }

    public long getTotalFilterMillis() {
        return totalFilterMillis;
    }

    public double getTotalFilterSeconds() {
        return getTotalFilterMillis() / 1000.0;
    }

    public void beginFilterInterval() {
        intervalStartMillis = System.currentTimeMillis();
        endMillis = intervalStartMillis;
        state = TaskState.filter;
    }

    public void endFilterInterval() {
        endMillis = System.currentTimeMillis();
        totalFilterMillis += (endMillis - intervalStartMillis);
        state = TaskState.other;
    }

    public long getTotalApplyMillis() {
        return totalApplyMillis;
    }

    public double getTotalApplySeconds() {
        return getTotalApplyMillis() / 1000.0;
    }

    public void beginApplyInterval() {
        intervalStartMillis = System.currentTimeMillis();
        endMillis = intervalStartMillis;
        state = TaskState.apply;
    }

    public void endApplyInterval() {
        endMillis = System.currentTimeMillis();
        totalApplyMillis += (endMillis - intervalStartMillis);
        state = TaskState.other;
    }

    public long getTotalOtherMillis() {
        long remaining = endMillis - startMillis - totalExtractMillis - totalFilterMillis - totalApplyMillis;
        return remaining;
    }

    public double getTotalOtherSeconds() {
        return getTotalOtherMillis() / 1000.0;
    }

    public TaskState getState() {
        return state;
    }

    public TaskProgress clone() {
        TaskProgress clone = new TaskProgress(this);
        return clone;
    }
}
