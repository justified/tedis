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
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.conf.ReplicatorRuntime;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.extractor.Extractor;
import com.taobao.common.tedis.replicator.plugin.PluginContext;
import com.taobao.common.tedis.replicator.plugin.PluginSpecification;
import com.taobao.common.tedis.replicator.plugin.ReplicatorPlugin;
import com.taobao.common.tedis.replicator.statemachine.event.EventDispatcher;

public class Stage implements ReplicatorPlugin {
    private static Logger logger = Logger.getLogger(Stage.class);
    // Stage elements.
    private String name;
    private PluginSpecification extractorSpec;
    private List<PluginSpecification> filterSpecs;
    private PluginSpecification applierSpec;
    private PluginContext pluginContext;
    private int blockCommitRowCount = 2;
    private boolean autoSync = false;

    // Read-only parameters.
    private StageProgressTracker progressTracker;

    // Task processing variables.
    StageTaskGroup taskGroup;
    int taskCount = 1;

    // Start-up parameters.
    String initialEventId;
    long applySkipCount = 0;
    private SortedSet<Long> seqnosToBeSkipped;

    private Pipeline pipeline = null;

    public Stage(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public String getName() {
        return name;
    }

    public int getTaskCount() {
        return taskCount;
    }

    public PluginSpecification getExtractorSpec() {
        return extractorSpec;
    }

    public List<PluginSpecification> getFilterSpecs() {
        return filterSpecs;
    }

    public PluginSpecification getApplierSpec() {
        return applierSpec;
    }

    public StageProgressTracker getProgressTracker() {
        return progressTracker;
    }

    public StageTaskGroup getTaskGroup() {
        return taskGroup;
    }

    public PluginContext getPluginContext() {
        return pluginContext;
    }

    public int getBlockCommitRowCount() {
        return blockCommitRowCount;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTaskCount(int taskCount) {
        this.taskCount = taskCount;
    }

    public void setExtractorSpec(PluginSpecification extractor) {
        this.extractorSpec = extractor;
    }

    public void setFilterSpecs(List<PluginSpecification> filters) {
        this.filterSpecs = filters;
    }

    public void setApplierSpec(PluginSpecification applier) {
        this.applierSpec = applier;
    }

    public void setBlockCommitRowCount(int blockCommitRowCount) {
        this.blockCommitRowCount = blockCommitRowCount;
    }

    public void setLoggingInterval(long loggingInterval) {
        this.progressTracker.setLoggingInterval(loggingInterval);
    }

    public String getInitialEventId() {
        return initialEventId;
    }

    public void setInitialEventId(String initialEventId) {
        this.initialEventId = initialEventId;
    }

    public long getApplySkipCount() {
        return applySkipCount;
    }

    public void setApplySkipCount(long applySkipCount) {
        this.applySkipCount = applySkipCount;
    }

    public boolean isAutoSync() {
        return autoSync;
    }

    public void setAutoSync(boolean autoSync) {
        this.autoSync = autoSync;
    }

    public synchronized List<TaskProgress> getTaskProgress() {
        return progressTracker.cloneTaskProgress();
    }

    public Extractor getExtractor0() {
        return taskGroup.getTask(0).getExtractor();
    }

    public synchronized void configure(PluginContext context) throws ReplicatorException, InterruptedException {
        this.pluginContext = context;
        if (taskCount < 1)
            throw new ReplicatorException("Stage task count may not be less than 1: stage=" + name + " taskCount=" + taskCount);

        progressTracker = new StageProgressTracker(name, taskCount);
        taskGroup = new StageTaskGroup(this, taskCount, progressTracker);
        taskGroup.configure(context);
    }

    public synchronized void prepare(PluginContext context) throws ReplicatorException, InterruptedException {
        taskGroup.prepare(context);
    }

    public synchronized void release(PluginContext context) throws ReplicatorException {
        // Need to ensure we are properly shut down.
        shutdown(true);

        taskGroup.release(context);
        progressTracker.release();
    }

    public synchronized void start(EventDispatcher dispatcher) throws ReplicatorException {
        // Set values for sequence numbers to be skipped.
        progressTracker.setApplySkipCount(applySkipCount);
        progressTracker.setSeqnosToBeSkipped(seqnosToBeSkipped);

        // Start tasks.
        taskGroup.start(dispatcher);
    }

    public synchronized void shutdown(boolean immediate) {
        try {
            taskGroup.stop(immediate);
        } catch (InterruptedException e) {
            logger.warn("Stage shutdown was interrupted");
        }
    }

    public synchronized boolean isShutdown() {
        return taskGroup.isShutdown();
    }

    public Future<ReplDBMSHeader> watchForProcessedSequenceNumber(long seqno, boolean terminate) throws InterruptedException {
        Future<ReplDBMSHeader> watch = progressTracker.watchForProcessedSequenceNumber(seqno, terminate);
        notifyThreads();
        return watch;
    }

    public Future<ReplDBMSHeader> watchForProcessedEventId(String eventId, boolean terminate) throws InterruptedException {
        Future<ReplDBMSHeader> watch = progressTracker.watchForProcessedEventId(eventId, terminate);
        notifyThreads();
        return watch;
    }

    public Future<ReplDBMSHeader> watchForProcessedTimestamp(Timestamp timestamp, boolean terminate) throws InterruptedException {
        Future<ReplDBMSHeader> watch = progressTracker.watchForProcessedTimestamp(timestamp, terminate);
        notifyThreads();
        return watch;
    }

    private void notifyThreads() {
        taskGroup.notifyTasks();
    }

    protected void configurePlugin(ReplicatorPlugin plugin, PluginContext context) throws ReplicatorException {
        ReplicatorRuntime.configurePlugin(plugin, context);
    }

    protected void preparePlugin(ReplicatorPlugin plugin, PluginContext context) throws ReplicatorException {
        ReplicatorRuntime.preparePlugin(plugin, context);
    }

    protected void releasePlugin(ReplicatorPlugin plugin, PluginContext context) {
        ReplicatorRuntime.releasePlugin(plugin, context);
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public void setApplySkipEvents(SortedSet<Long> seqnos) {
        this.seqnosToBeSkipped = seqnos;
    }
}