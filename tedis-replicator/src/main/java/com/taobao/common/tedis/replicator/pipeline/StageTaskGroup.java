/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.applier.Applier;
import com.taobao.common.tedis.replicator.applier.ApplierWrapper;
import com.taobao.common.tedis.replicator.applier.RawApplier;
import com.taobao.common.tedis.replicator.conf.ReplicatorRuntime;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.extractor.Extractor;
import com.taobao.common.tedis.replicator.extractor.ExtractorWrapper;
import com.taobao.common.tedis.replicator.extractor.RawExtractor;
import com.taobao.common.tedis.replicator.filter.Filter;
import com.taobao.common.tedis.replicator.plugin.PluginContext;
import com.taobao.common.tedis.replicator.plugin.PluginSpecification;
import com.taobao.common.tedis.replicator.plugin.ReplicatorPlugin;
import com.taobao.common.tedis.replicator.statemachine.event.EventDispatcher;

/**
 * This class encapsulates a group of tasks that run together in a single stage.
 * It handles life-cycle operations ranging from instantiation to release.
 */
public class StageTaskGroup implements ReplicatorPlugin {
    private static Logger logger = Logger.getLogger(StageTaskGroup.class);
    private Stage stage;
    private int taskCount;
    private StageProgressTracker tracker;

    private SingleThreadStageTask[] tasks;

    private boolean shutdown = false;

    private HashMap<String, Thread> threadMap = new HashMap<String, Thread>();

    public StageTaskGroup(Stage stage, int taskCount, StageProgressTracker tracker) {
        this.taskCount = taskCount;
        this.stage = stage;
        this.tracker = tracker;
    }

    public int getTaskCount() {
        return taskCount;
    }

    public SingleThreadStageTask[] getTasks() {
        return tasks;
    }

    public SingleThreadStageTask getTask(int id) {
        return tasks[id];
    }

    public void configure(PluginContext context) throws ReplicatorException, InterruptedException {
        // Instantiate and configure each task.
        logger.info("Instantiating and configuring tasks for stage: " + stage.getName());
        tasks = new SingleThreadStageTask[taskCount];
        for (int i = 0; i < taskCount; i++) {
            // Instantiate the task.
            tasks[i] = new SingleThreadStageTask(stage, i);

            // Instantiate and configure the extractor. Parallel extractors
            // get the task ID.
            ReplicatorPlugin extractor = this.stage.getExtractorSpec().instantiate(i);
            if (extractor instanceof RawExtractor) {
                extractor = new ExtractorWrapper((RawExtractor) extractor);
            }

            extractor.configure(context);
            tasks[i].setExtractor((Extractor) extractor);

            // Instantiate and configure filters.
            List<Filter> filterList = new ArrayList<Filter>(stage.getFilterSpecs().size());
            for (PluginSpecification filter : stage.getFilterSpecs()) {
                Filter f = (Filter) filter.instantiate(i);
                f.configure(context);
                filterList.add(f);
            }
            tasks[i].setFilters(filterList);

            // Instantiate and configure the applier.
            ReplicatorPlugin applier = this.stage.getApplierSpec().instantiate(i);
            if (applier instanceof RawApplier) {
                ((RawApplier) applier).setTaskId(i);
                applier = new ApplierWrapper((RawApplier) applier);
            }
            applier.configure(context);
            tasks[i].setApplier((Applier) applier);
        }

    }

    public void prepare(PluginContext context) throws ReplicatorException, InterruptedException {
        // Prepare components within each task.
        logger.info("Preparing tasks for stage: " + stage.getName());
        for (int i = 0; i < taskCount; i++) {
            // Prepare components.
            logger.debug("Preparing task: " + i);
            SingleThreadStageTask task = tasks[i];

            ReplicatorRuntime.preparePlugin(task.getExtractor(), context);

            for (Filter f : task.getFilters()) {
                ReplicatorRuntime.preparePlugin(f, context);
            }

            ReplicatorRuntime.preparePlugin(task.getApplier(), context);

            // Get the starting event data and position extractor.
            logger.debug("Looking up last applied event to position extractor");
            ReplDBMSHeader lastHeader = task.getApplier().getLastEvent();
            if (lastHeader == null || lastHeader.getSeqno() < 0) {
                logger.warn("[" + task.getName() + "] " + "Last event data not available; " + "Setting extractor to current position");
                task.getExtractor().setLastEvent(null);
            } else {
                logger.info("[" + task.getName() + "] " + "Setting extractor position: seqno=" + lastHeader.getSeqno() + " event=" + lastHeader.getEventId());
                task.getExtractor().setLastEvent(lastHeader);
            }
        }
    }

    public void release(PluginContext context) {
        // Release components within each task.
        logger.info("Releasing tasks for stage: " + stage.getName());
        for (int i = 0; i < taskCount; i++) {
            logger.debug("Releasing task: " + i);
            ReplicatorRuntime.releasePlugin(tasks[i].getExtractor(), context);

            for (Filter f : tasks[i].getFilters()) {
                ReplicatorRuntime.releasePlugin(f, context);
            }

            ReplicatorRuntime.releasePlugin(tasks[i].getApplier(), context);
        }
    }

    /**
     * Start all tasks in the group.
     *
     * @throws ReplicatorException
     */
    public void start(EventDispatcher dispatcher) throws ReplicatorException {
        for (SingleThreadStageTask task : tasks) {
            if (threadMap.get(task.getName()) != null) {
                logger.warn("Task has already been started: " + task.getName());
                return;
            }

            // If we have an initial event ID, set that now to override
            // any default value.
            if (stage.getInitialEventId() != null)
                task.getExtractor().setLastEventId(stage.getInitialEventId());

            // Create and start the processing thread.
            try {
                task.setEventDispatcher(dispatcher);
                task.setSchedule(new SimpleSchedule(stage, task));
                Thread stageThread = new Thread(task);
                stageThread.setName(task.getName());
                threadMap.put(task.getName(), stageThread);
                stageThread.start();
            } catch (Throwable t) {
                String message = "Failed to start stage task";
                logger.error(message, t);
                throw new ReplicatorException(message, t);
            }
        }
    }

    /**
     * Stop all tasks in the group.
     *
     * @param immediate
     *            If true, interrupt and exit immediately
     */
    public synchronized void stop(boolean immediate) throws InterruptedException {
        // Do not shut down twice.
        if (shutdown)
            return;

        // Wait for stages to shut down.
        for (SingleThreadStageTask task : tasks) {
            Thread stageThread = threadMap.remove(task.getName());
            if (stageThread != null) {
                try {
                    // We have to interrupt for non-parallel store or if
                    // this an immediate shutdown.
                    if (immediate) {
                        task.cancel();
                        stageThread.interrupt();
                        stageThread.join();
                    } else
                        stageThread.join();
                } catch (InterruptedException e) {
                    logger.warn("Interrupted while waiting for stage thread to exit");
                }
            }
        }

        if (threadMap.size() == 0) {
            shutdown = true;
        }
    }

    /**
     * Reports that a task has shut down. If no threads remain, we report that
     * the stage has shut down.
     */
    public void reportTaskShutdown(Thread taskThread, SingleThreadStageTask task) {
        if (logger.isDebugEnabled()) {
            logger.debug("Recording task shutdown: thread=" + taskThread + " task=" + task);
        }
        if (threadMap.remove(task.getName()) != null) {
            if (logger.isDebugEnabled())
                logger.debug("Task shutdown: " + task.getName());
        }
        if (threadMap.size() == 0) {
            this.shutdown = true;
            if (logger.isDebugEnabled())
                logger.debug("Task group has shut down following last task end");
        }
    }

    /**
     * Interrupts currently running tasks.
     */
    public void notifyTasks() {
        if (tracker.shouldInterruptTask()) {
            for (Thread t : threadMap.values()) {
                t.interrupt();
            }
        }
    }

    /**
     * Returns true if the stage has stopped.
     */
    public synchronized boolean isShutdown() {
        return shutdown;
    }
}