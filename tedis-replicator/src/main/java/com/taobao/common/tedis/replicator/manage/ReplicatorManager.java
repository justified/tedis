/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.manage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;

import javax.management.Notification;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ErrorNotification;
import com.taobao.common.tedis.replicator.OutOfSequenceNotification;
import com.taobao.common.tedis.replicator.PropertyException;
import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.ReplicatorProperties;
import com.taobao.common.tedis.replicator.conf.PropertiesManager;
import com.taobao.common.tedis.replicator.conf.ReplicatorConf;
import com.taobao.common.tedis.replicator.conf.ReplicatorRuntime;
import com.taobao.common.tedis.replicator.conf.ReplicatorRuntimeConf;
import com.taobao.common.tedis.replicator.manage.events.GoOfflineEvent;
import com.taobao.common.tedis.replicator.manage.events.OfflineNotification;
import com.taobao.common.tedis.replicator.statemachine.Action;
import com.taobao.common.tedis.replicator.statemachine.Entity;
import com.taobao.common.tedis.replicator.statemachine.EntityAdapter;
import com.taobao.common.tedis.replicator.statemachine.Event;
import com.taobao.common.tedis.replicator.statemachine.EventTypeGuard;
import com.taobao.common.tedis.replicator.statemachine.FiniteStateException;
import com.taobao.common.tedis.replicator.statemachine.Guard;
import com.taobao.common.tedis.replicator.statemachine.State;
import com.taobao.common.tedis.replicator.statemachine.StateChangeListener;
import com.taobao.common.tedis.replicator.statemachine.StateMachine;
import com.taobao.common.tedis.replicator.statemachine.StateTransitionLatch;
import com.taobao.common.tedis.replicator.statemachine.StateTransitionMap;
import com.taobao.common.tedis.replicator.statemachine.StateType;
import com.taobao.common.tedis.replicator.statemachine.Transition;
import com.taobao.common.tedis.replicator.statemachine.TransitionFailureException;
import com.taobao.common.tedis.replicator.statemachine.TransitionNotFoundException;
import com.taobao.common.tedis.replicator.statemachine.TransitionRollbackException;
import com.taobao.common.tedis.replicator.statemachine.event.EventCompletionListener;
import com.taobao.common.tedis.replicator.statemachine.event.EventDispatcher;
import com.taobao.common.tedis.replicator.statemachine.event.EventDispatcherTask;
import com.taobao.common.tedis.replicator.statemachine.event.EventRequest;
import com.taobao.common.tedis.replicator.statemachine.event.EventStatus;

public class ReplicatorManager implements ReplicatorContext, StateChangeListener, EventCompletionListener {
    private static final int ADMIN_THREAD_LIMIT = 100;

    // Name of this replication service.
    private String serviceName;

    // When the service started.
    private long startTimeMillis = System.currentTimeMillis();

    // Configuration is stored in the ReplicatorRuntime.
    private ReplicatorProperties properties = null;
    private PropertiesManager propertiesManager = null;

    // Subsystems
    private EventDispatcherTask eventDispatcher = null;

    // State machine
    private StateTransitionMap stmap = null;
    private StateMachine sm = null;
    private long stateChangeTimeMillis = 0;

    // Thread pool for administrative operations like waiting for a state.
    private ExecutorService adminThreadPool = Executors.newFixedThreadPool(ADMIN_THREAD_LIMIT);

    // Pending error, if any.
    private String pendingError = null;
    private String pendingErrorCode = null;
    private String pendingExceptionMessage = null;
    private long pendingErrorSeqno = -1;
    private String pendingErrorEventId = null;

    // Monitoring and management
    private static Logger logger = Logger.getLogger(ReplicatorManager.class);
    private static Logger endUserLog = Logger.getLogger("tedis.userLog");

    public static final int REPL = 1;
    public static final int FLUSH = 2;

    // Open replicator plugin
    private OpenReplicatorPlugin openReplicator;

    private CountDownLatch doneLatch = null;

    public ReplicatorManager(String serviceName) throws Exception {
        this.serviceName = serviceName;
        logger.info("Configuring state machine for replication service: " + serviceName);
        Action flushAction = new FlushAction();
        Action stopAction = new StopAction();
        Action goOfflineAction = new GoOfflineAction();
        Action deferredOfflineAction = new DeferredOfflineAction();
        Action offlineToOnlineAction = new OfflineToOnlineAction();
        Action configureAction = new ConfigureAction();
        Action errorClearAction = new ErrorClearAction();
        Action startToOfflineAction = new StartToOfflineAction();
        Action errorShutdownAction = new ErrorShutdownAction();
        Action errorRecordingAction = new ErrorRecordingAction();
        Action extendedAction = new ExtendedAction();

        // Define replicator states.
        stmap = new StateTransitionMap();
        State start = new State("START", StateType.START);
        State offline = new State("OFFLINE", StateType.ACTIVE);
        State offlineNormal = new State("NORMAL", StateType.ACTIVE, offline);
        State offlineConfiguring = new State("CONFIGURING", StateType.ACTIVE, offline);
        State offlineError = new State("ERROR", StateType.ACTIVE, offline, errorShutdownAction, errorClearAction);
        State online = new State("ONLINE", StateType.ACTIVE);
        State end = new State("END", StateType.END, stopAction, null);

        stmap.addState(start);
        stmap.addState(offline);
        stmap.addState(offlineNormal);
        stmap.addState(offlineConfiguring);
        stmap.addState(offlineError);
        stmap.addState(online);
        stmap.addState(end);

        // Designate error state.
        stmap.setErrorState(offlineError);

        // Define guard conditions for event types.
        Guard startGuard = new EventTypeGuard(StartEvent.class);
        Guard stopGuard = new EventTypeGuard(StopEvent.class);
        Guard configureGuard = new EventTypeGuard(ConfigureEvent.class);
        Guard configuredGuard = new EventTypeGuard(ConfiguredNotification.class);
        Guard goOnlineGuard = new EventTypeGuard(GoOnlineEvent.class);
        Guard outOfSequenceGuard = new EventTypeGuard(OutOfSequenceNotification.class);
        Guard goOfflineGuard = new EventTypeGuard(GoOfflineEvent.class);
        Guard offlineGuard = new EventTypeGuard(OfflineNotification.class);
        Guard deferredOfflineGuard = new EventTypeGuard(DeferredOfflineEvent.class);
        Guard flushGuard = new EventTypeGuard(FlushEvent.class);
        Guard errorGuard = new EventTypeGuard(ErrorNotification.class);
        Guard extendedActionGuard = new ExtendedActionEventGuard();

        // START state can transition to OFFLINE and END.
        stmap.addTransition(new Transition("START-TO-OFFLINE", startGuard, start, startToOfflineAction, offlineNormal));
        stmap.addTransition(new Transition("START-STOP", stopGuard, start, null, end));

        // OFFLINE state has 2 substates.
        // 1. NORMAL -- Normal non-active state.
        // 2. ERROR -- An error has occurred.
        // All offline states can transition to offline error and process extended commands.
        stmap.addTransition(new Transition("OFFLINE-NORMAL", offlineGuard, offline, null, offlineNormal));
        stmap.addTransition(new Transition("OFFLINE-ERROR", errorGuard, offline, errorRecordingAction, offlineError));
        stmap.addTransition(new Transition("OFFLINE-EXTENDED", extendedActionGuard, offline, extendedAction, offline));

        // OFFLINE:NORMAL can transition to any of the following states.
        stmap.addTransition(new Transition("OFFLINE-OFFLINE-1", goOfflineGuard, offlineNormal, null, offlineNormal));
        stmap.addTransition(new Transition("OFFLINE-OFFLINE-2", deferredOfflineGuard, offlineNormal, null, offlineNormal));
        stmap.addTransition(new Transition("OFFLINE-CONFIGURE", configureGuard, offlineNormal, configureAction, offlineConfiguring));
        stmap.addTransition(new Transition("OFFLINE-ONLINE", goOnlineGuard, offlineNormal, offlineToOnlineAction, online));
        stmap.addTransition(new Transition("OFFLINE-STOP", stopGuard, offlineNormal, null, end));

        // OFFLINE:CONFIGURING can transition back to following states.
        stmap.addTransition(new Transition("CONFIGURE-OFFLINE", configuredGuard, offlineConfiguring, null, offlineNormal));

        // OFFLINE:ERROR can transition to any of the following states.
        stmap.addTransition(new Transition("ERROR-NORMAL-1", goOfflineGuard, offlineError, null, offlineNormal));
        stmap.addTransition(new Transition("ERROR-NORMAL-2", deferredOfflineGuard, offlineError, null, offlineNormal));
        stmap.addTransition(new Transition("ERROR-CONFIGURE", configureGuard, offlineError, configureAction, offlineNormal));
        stmap.addTransition(new Transition("ERROR-ONLINE", goOnlineGuard, offlineError, offlineToOnlineAction, online));

        // ONLINE can transition to any of the following states;
        stmap.addTransition(new Transition("ONLINE-SHUTDOWN", deferredOfflineGuard, online, deferredOfflineAction, online));
        stmap.addTransition(new Transition("ONLINE-SHUTDOWN", goOfflineGuard, online, goOfflineAction, offline));
        stmap.addTransition(new Transition("ONLINE-OUTOFSEQUENCE", outOfSequenceGuard, online, null, online));
        stmap.addTransition(new Transition("ONLINE-ERROR", errorGuard, online, errorRecordingAction, offlineError));
        stmap.addTransition(new Transition("FLUSH", flushGuard, online, flushAction, online));
        stmap.addTransition(new Transition("ONLINE-EXTENDED", extendedActionGuard, online, extendedAction, online));

        stmap.build();
        sm = new StateMachine(stmap, new EntityAdapter(this));
        sm.addListener(this);

        // Start the event dispatcher.
        eventDispatcher = new EventDispatcherTask(sm);
        eventDispatcher.setListener(this);
        eventDispatcher.start(serviceName + "-dispatcher");

        // Start the property manager.
        ReplicatorRuntimeConf runtimeConf = ReplicatorRuntimeConf.getConfiguration(serviceName);
        propertiesManager = new PropertiesManager(runtimeConf.getReplicatorProperties());
        propertiesManager.loadProperties();
    }

    public void stateChanged(Entity entity, State oldState, State newState) {
        Notification notification = new Notification("ReplicatorStateChange", this, 0);
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("oldState", oldState.getName());
        map.put("newState", newState.getName());
        notification.setUserData(map);
        stateChangeTimeMillis = System.currentTimeMillis();
        logger.info("Sent State Change Notification " + oldState.getName() + " -> " + newState.getName());
        endUserLog.info("State changed " + oldState.getName() + " -> " + newState.getName());
    }

    public Object onCompletion(Event event, EventStatus status) throws InterruptedException {
        Object annotation = null;
        if (status.isSuccessful()) {
            if (logger.isDebugEnabled())
                logger.debug("Applied event: " + event.getClass().getSimpleName());
        } else if (status.isCancelled()) {
            logger.warn("Event processing was cancelled: " + event.getClass().getSimpleName());
        } else if (status.getException() != null) {
            Throwable t = status.getException();

            if (t instanceof TransitionNotFoundException) {
                // This is just a warning. We received an event that is
                // inappropriate for the current state.
                TransitionNotFoundException e = (TransitionNotFoundException) t;
                StringBuffer msg = new StringBuffer();
                msg.append("Received irrelevant event for current state: state=");
                msg.append(e.getState().getName());
                msg.append(" event=");
                msg.append(e.getEvent().getClass().getSimpleName());
                logger.warn(msg.toString());
                endUserLog.warn(msg.toString());
                annotation = new ReplicatorStateException("Operation irrelevant in current state");
            } else if (t instanceof TransitionRollbackException) {
                // A transition could not complete and rolled back to the
                // original state.
                TransitionRollbackException e = (TransitionRollbackException) t;
                StringBuffer msg = new StringBuffer();
                msg.append("State transition could not complete and was rolled back: state=");
                msg.append(e.getTransition().getInput().getName());
                msg.append(" transition=");
                msg.append(e.getTransition().getName());
                msg.append(" event=");
                msg.append(e.getEvent().getClass().getSimpleName());
                String errMsg = msg.toString();
                endUserLog.error(errMsg);
                displayErrorMessages(e);
                annotation = getStateMachineException(e, errMsg);
            } else if (t instanceof TransitionFailureException) {
                // A transition failed, causing the replicator to go into the
                // OFFLINE:ERROR state.
                TransitionFailureException e = (TransitionFailureException) t;
                StringBuffer msg = new StringBuffer();
                msg.append("State transition failed causing emergency recovery: state=");
                msg.append(e.getTransition().getInput().getName());
                msg.append(" transition=");
                msg.append(e.getTransition().getName());
                msg.append(" event=");
                msg.append(e.getEvent().getClass().getSimpleName());
                String errMsg = msg.toString();
                endUserLog.error(errMsg);
                displayErrorMessages(e);
                annotation = getStateMachineException(e, errMsg);
            } else if (t instanceof FiniteStateException) {
                // Should not exit here, this event may be result of
                // user operation
                logger.error("Unexpected state transition processing error", t);
                annotation = new ReplicatorException("Operation failed unexpectedly--see log for details", t);
            } else {
                // We probably have some sort of bug. We need to wrap it in a
                // replicator exception.
                logger.error("Unexpected processing error", t);
                annotation = new ReplicatorException("Operation failed unexpectedly--see log for details", t);
            }
        }
        return annotation;
    }

    private ReplicatorStateException getStateMachineException(FiniteStateException e, String msg) {
        ReplicatorStateException replicatorStateException = new ReplicatorStateException(msg, e);
        if (e.getCause() != null && e.getCause() instanceof ReplicatorException) {
            ReplicatorException exc = (ReplicatorException) e.getCause();
            replicatorStateException.setOriginalErrorMessage(exc.getOriginalErrorMessage());
            replicatorStateException.setExtraData(exc.getExtraData());
        }
        return replicatorStateException;
    }

    /**
     * Signals that the replicator should start.
     */
    class StartEvent extends Event {
        public StartEvent() {
            super(null);
        }
    }

    /**
     * Signals that the replicator should reconfigure properties.
     */
    class ConfigureEvent extends Event {
        /** If props are null, re-read replicator properties. */
        public ConfigureEvent(ReplicatorProperties props) {
            super(props);
        }
    }

    /**
     * Signals that the replicator should exit.
     */
    class StopEvent extends Event {
        public StopEvent() {
            super(null);
        }
    }

    /**
     * This class defines a FlushEvent, which processes a request to synchronize
     * the database with the replicator.
     */
    class FlushEvent extends Event {
        String eventId;

        public FlushEvent() {
            super(null);
        }

        public void setEventFuture(String event) {
            eventId = event;
        }
    }

    /**
     * Signals that the replicator should move to the online state.
     */
    public class GoOnlineEvent extends Event {
        private ReplicatorProperties params;

        public GoOnlineEvent(ReplicatorProperties params) {
            super(null);
            this.params = params;
        }

        public ReplicatorProperties getParams() {
            return params;
        }
    }

    /**
     * Request to send replicator offline at a later time.
     */
    class DeferredOfflineEvent extends Event {
        private ReplicatorProperties params;

        public DeferredOfflineEvent(ReplicatorProperties params) {
            super(null);
            this.params = params;
        }

        public ReplicatorProperties getParams() {
            return params;
        }
    }

    class ConfiguredNotification extends Event {
        public ConfiguredNotification() {
            super(null);
        }
    }

    /**
     * Guard for an extended event. We accept the event if the current state is
     * a match with the extended event pattern.
     */
    class ExtendedActionEventGuard implements Guard {
        public boolean accept(Event event, Entity entity, State state) {
            if (!(event instanceof ExtendedActionEvent)) {
                return false;
            }
            ExtendedActionEvent extendedEvent = (ExtendedActionEvent) event;
            Matcher m = extendedEvent.getStatePattern().matcher(state.getName());
            return m.matches();
        }
    };

    /**
     * Action to process an error. This is used by a normal transition triggered
     * by receipt of an ErrorNotification. It extracts and stores the error
     * message.
     */
    class ErrorRecordingAction implements Action {
        public void doAction(Event event, Entity entity, Transition transition, int actionType) {
            // Log the error condition.
            ErrorNotification en = (ErrorNotification) event;

            displayErrorMessages(en);
            String message = "Received error notification, shutting down services :\n" + en.getUserMessage();
            if (en.getThrowable() instanceof ReplicatorException && ((ReplicatorException) en.getThrowable()).getExtraData() != null) {
                message += "\n" + ((ReplicatorException) en.getThrowable()).getExtraData();
            }
            logger.error(message, en.getThrowable());

            // Store the user error message.
            pendingError = en.getUserMessage();
            pendingExceptionMessage = en.getThrowable().getMessage();
            if (en.getThrowable() instanceof ReplicatorException) {
                ReplicatorException exc = (ReplicatorException) en.getThrowable();
                if (exc.getExtraData() != null)
                    pendingExceptionMessage += "\n" + exc.getExtraData();
            }
            pendingErrorSeqno = en.getSeqno();
            pendingErrorEventId = en.getEventId();
        }

    }

    private void displayErrorMessages(ErrorNotification event) {
        endUserLog.error(event.getUserMessage());
        Throwable error = event.getThrowable();
        if (error instanceof ReplicatorException && ((ReplicatorException) error).getExtraData() != null) {
            for (String line : ((ReplicatorException) error).getExtraData().split("\n")) {
                endUserLog.error(line);
            }

        }
    }

    // Echo error messages to the user log.
    private void displayErrorMessages(Exception exception) {
        String message = exception.getMessage();
        endUserLog.error(String.format("[%s] message: %s", exception.getClass().getSimpleName(), message));

        Throwable error = exception.getCause();
        boolean stop = false;
        while (error != null) {
            if (!(error instanceof ReplicatorException)) {
                // Break after the second non ReplicatorException in a
                // row is found
                if (stop)
                    break;

                stop = true;
            } else
                stop = false;

            String message2 = error.getMessage();
            endUserLog.error(String.format("[%s] message: %s", error.getClass().getSimpleName(), message2));
            error = error.getCause();
        }
    }

    /**
     * Action to shut down services following an error so that we can restart cleanly.
     */
    class ErrorShutdownAction implements Action {
        public void doAction(Event event, Entity entity, Transition transition, int actionType) {
            // Close down services as cleanly as possible.
            logger.warn("Performing emergency service shutdown");
            try {
                if (openReplicator != null)
                    openReplicator.offline(new ReplicatorProperties());
            } catch (Throwable e) {
                logger.error("Service shutdown failed...Services may be active", e);
            }

            logger.info("All internal services are shut down; replicator ready for recovery");
        }
    }

    /* Action to clear pending error message. */
    class ErrorClearAction implements Action {
        public void doAction(Event event, Entity entity, Transition transition, int actionType) {
            pendingError = null;
            pendingExceptionMessage = null;
            pendingErrorSeqno = -1;
            pendingErrorEventId = null;
        }
    };

    /*
     * Action in transition from START to OFFLINE state.
     */
    class StartToOfflineAction implements Action {
        public void doAction(Event event, Entity entity, Transition transition, int actionType) throws TransitionRollbackException, TransitionFailureException {
            // Load properties file.
            loadProperties(event, entity, transition, actionType);

            // Run configuration.
            try {
                doConfigure();
            } catch (ReplicatorException e) {
                pendingError = "Replicator configuration failed";
                pendingExceptionMessage = e.getMessage();
                if (logger.isDebugEnabled())
                    logger.debug(pendingError, e);
                throw new TransitionFailureException(pendingError, event, entity, transition, actionType, e);
            }
        }
    }

    /*
     * Action to configure properties by either rereading them or setting all
     * properties from outside.
     */
    class ConfigureAction implements Action {
        public void doAction(Event event, Entity entity, Transition transition, int actionType) throws TransitionRollbackException {
            ReplicatorProperties newProps = (ReplicatorProperties) ((ConfigureEvent) event).getData();
            if (newProps == null)
                loadProperties(event, entity, transition, actionType);
            else
                properties = newProps;

            try {
                doConfigure();
            } catch (ReplicatorException e) {
                logger.error("configuration failed for: " + e);
                throw new RuntimeException(e);
            }

            try {
                eventDispatcher.put(new ConfiguredNotification());
            } catch (InterruptedException e) {
                // TODO Log this?
            }
        }
    };

    /**
     * Action to handle an extended action event, which is basically an enclosed
     * action.
     */
    class ExtendedAction implements Action {
        public void doAction(Event event, Entity entity, Transition transition, int actionType) throws TransitionRollbackException, TransitionFailureException, InterruptedException {
            // This is a pass-through to the enclosed action.
            ExtendedActionEvent extendedEvent = (ExtendedActionEvent) event;
            Action action = extendedEvent.getExtendedAction();
            action.doAction(event, entity, transition, actionType);
        }
    };

    /*
     * Action in transition from OFFLINE to SYNCHRONIZING state.
     */
    class OfflineToOnlineAction implements Action {
        public void doAction(Event event, Entity entity, Transition transition, int actionType) throws TransitionRollbackException, TransitionFailureException {
            try {
                ReplicatorProperties params;
                if (event instanceof GoOnlineEvent) {
                    GoOnlineEvent goOnlineEvent = (GoOnlineEvent) event;
                    params = goOnlineEvent.getParams();
                } else
                    params = new ReplicatorProperties();

                openReplicator.online(params);
            } catch (ReplicatorException e) {
                // Pending error is correctly set.
                pendingError = "Replicator unable to go online due to error";
                pendingExceptionMessage = e.getMessage();
                if (logger.isDebugEnabled())
                    logger.debug(pendingError, e);
                throw new TransitionFailureException(pendingError, event, entity, transition, actionType, e);
            } catch (Throwable e) {
                pendingError = "Replicator service start-up failed due to underlying error";
                pendingExceptionMessage = e.toString();
                logger.error(String.format("%s, reason=%s", pendingError, e));
                throw new TransitionFailureException(pendingError, event, entity, transition, actionType, e);
            }
        }
    };

    /*
     * Action for transition from any state to OFFLINE state.
     */
    class GoOfflineAction implements Action {
        public void doAction(Event event, Entity entity, Transition transition, int actionType) throws TransitionFailureException {
            try {
                GoOfflineEvent goOfflineEvent = (GoOfflineEvent) event;
                openReplicator.offline(goOfflineEvent.getParams());
            } catch (Throwable e) {
                pendingError = "Replicator service shutdown failed due to underlying error";
                pendingExceptionMessage = e.toString();
                logger.error(pendingError, e);
                throw new TransitionFailureException(pendingError, event, entity, transition, actionType, e);
            }
        }
    }

    /*
     * Action for handling deferred offline action.
     */
    class DeferredOfflineAction implements Action {
        public void doAction(Event event, Entity entity, Transition transition, int actionType) throws TransitionFailureException {
            try {
                DeferredOfflineEvent deferredOfflineEvent = (DeferredOfflineEvent) event;
                openReplicator.offlineDeferred(deferredOfflineEvent.getParams());
            } catch (Throwable e) {
                pendingError = "Deferred offline request failed due to underlying error";
                pendingExceptionMessage = e.toString();
                logger.error(pendingError, e);
                throw new TransitionFailureException(pendingError, event, entity, transition, actionType, e);
            }
        }
    }

    /*
     * Action to trigger a flush of the master. This may roll back if we are not
     * in the master role.
     */
    class FlushAction implements Action {
        public void doAction(Event event, Entity entity, Transition transition, int actionType) throws TransitionFailureException, TransitionRollbackException {
            // Flush is only permitted for masters.
            try {
                // Ask the plugin to perform a flush operation.
                FlushEvent flushEvent = (FlushEvent) event;
                String future = openReplicator.flush(0);
                flushEvent.setEventFuture(future);
            } catch (Exception e) {
                pendingError = "Unable to process flush request";
                pendingExceptionMessage = e.getMessage();
                throw new TransitionFailureException(pendingError, event, entity, transition, actionType, e);
            }

        }
    };

    class StopAction implements Action {
        public void doAction(Event event, Entity entity, Transition transition, int actionType) throws TransitionFailureException {
            // do nothing
        }
    };

    public EventDispatcher getEventDispatcher() {
        return this.eventDispatcher;
    }

    public boolean isAlive() {
        return true;
    }

    public String getVersion() {
        return "No version available";
    }

    public String getSimpleServiceName() {
        if (serviceName != null) {
            return serviceName;
        }
        return null;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getSourceId() {
        return properties.getString(ReplicatorConf.SOURCE_ID);
    }

    public double getUptimeSeconds() {
        return (System.currentTimeMillis() - this.startTimeMillis) / 1000.0;
    }

    public String getState() {
        return this.sm.getState().getName();
    }

    public double getTimeInStateSeconds() {
        return (System.currentTimeMillis() - stateChangeTimeMillis) / 1000.0;
    }

    public long getStateChangeTimeMillis() {
        return stateChangeTimeMillis;
    }

    public String getPendingErrorCode() {
        return pendingErrorCode;
    }

    public String getPendingError() {
        return pendingError;
    }

    public String getPendingExceptionMessage() {
        return pendingExceptionMessage;
    }

    public String getMaxSeqNo() throws Exception {
        return status().get(Replicator.APPLIED_LAST_SEQNO);
    }

    public String[] getMinMaxSeqNo() throws Exception {
        String[] pair = { getMaxSeqNo(), getMinSeqNo() };
        return pair;
    }

    public String getMinSeqNo() throws Exception {
        // TODO Add this information to the status call.
        return "-1";
    }

    public void configure(Map<String, String> props) throws Exception {
        try {
            ReplicatorProperties rp;
            if (props == null)
                rp = null;
            else {
                rp = new ReplicatorProperties(props);
                logger.info("Updating properties from remote client");
                if (logger.isDebugEnabled()) {
                    logger.debug("New properties: " + props.toString());
                }
            }
            configure(rp);
        } catch (Exception e) {
            logger.error("Configure operation failed", e);
            throw new Exception("Configure operation failed: " + e.getMessage());
        }
    }

    public Map<String, String> status() throws Exception {
        try {
            // Get status from plugin
            HashMap<String, String> pluginStatus = openReplicator.status();

            // Convert old plugin values so we don't mess up existing script plug-ins.
            convertOldValue(pluginStatus, Replicator.APPLIED_LAST_SEQNO, OpenReplicatorPlugin.STATUS_LAST_APPLIED);

            // Following are standard variables over and above values provided by plugin.
            pluginStatus.put(Replicator.SERVICE_NAME, serviceName);
            pluginStatus.put(Replicator.SIMPLE_SERVICE_NAME, getSimpleServiceName());
            pluginStatus.put(Replicator.SOURCEID, getSourceId());
            pluginStatus.put(Replicator.UPTIME_SECONDS, Double.toString(getUptimeSeconds()));
            pluginStatus.put(Replicator.TIME_IN_STATE_SECONDS, Double.toString(getTimeInStateSeconds()));
            pluginStatus.put(Replicator.STATE, getState());

            pluginStatus.put(Replicator.PENDING_EXCEPTION_MESSAGE, (getPendingExceptionMessage() == null ? "NONE" : getPendingExceptionMessage()));
            pluginStatus.put(Replicator.PENDING_ERROR_CODE, (getPendingErrorCode() == null ? "NONE" : getPendingErrorCode()));
            pluginStatus.put(Replicator.PENDING_ERROR, (getPendingError() == null ? "NONE" : getPendingError()));
            pluginStatus.put(Replicator.PENDING_ERROR_SEQNO, Long.toString(pendingErrorSeqno));
            pluginStatus.put(Replicator.PENDING_ERROR_EVENTID, (pendingErrorEventId == null ? "NONE" : pendingErrorEventId));
            pluginStatus.put(Replicator.RESOURCE_PRECEDENCE, properties.getString(ReplicatorConf.RESOURCE_PRECEDENCE, ReplicatorConf.RESOURCE_PRECEDENCE_DEFAULT, true));
            pluginStatus.put(Replicator.CURRENT_TIME_MILLIS, Long.toString(System.currentTimeMillis()));

            if (logger.isDebugEnabled())
                logger.debug("plugin status: " + pluginStatus.toString());

            // Return the finalized status values.
            return pluginStatus;
        } catch (Exception e) {
            logger.error("Status operation failed", e);
            throw new Exception("Status operation failed: " + e.getMessage());
        }
    }

    private void convertOldValue(Map<String, String> pluginStatus, String newName, String oldName) {
        String value = pluginStatus.get(oldName);
        if (value != null) {
            pluginStatus.remove(oldName);
            pluginStatus.put(newName, value);
        }
    }

    public ReplicatorProperties getStatus() throws Exception {
        return new ReplicatorProperties(status());
    }

    public List<Map<String, String>> statusList(String name) throws Exception {
        return openReplicator.statusList(name);
    }

    public void start() throws Exception {
        try {
            handleEventSynchronous(new StartEvent());
            if (sm.getState().getName().equals("OFFLINE:NORMAL")) {
                // Runtime does not exist yet so we need to check properties directly.
                boolean autoEnabled = new Boolean(properties.getBoolean(ReplicatorConf.AUTO_ENABLE));
                if (autoEnabled) {
                    logger.info("Replicator auto-enabling is engaged; going online automatically");
                    online();
                }
            }
        } catch (Exception e) {
            logger.error("Start operation failed", e);
            throw new Exception("Start operation failed: " + e.getMessage());
        }

        this.doneLatch = new CountDownLatch(1);
    }

    public void stop() throws Exception {
        try {
            handleEventSynchronous(new StopEvent());

            if (doneLatch != null) {
                doneLatch.countDown();
            }
        } catch (Exception e) {
            logger.error("Stop operation failed", e);
            throw new Exception(e.toString());
        }
    }

    public void online() throws Exception {
        online(new HashMap<String, String>());
    }

    public void online(Map<String, String> map) throws Exception {
        ReplicatorProperties prop = new ReplicatorProperties(map);
        GoOnlineEvent goOnlineEvent = new GoOnlineEvent(prop);
        try {
            handleEventSynchronous(goOnlineEvent);
        } catch (ReplicatorException e) {
            String message = "Online operation failed";
            logger.error(message, e);
            if (e.getOriginalErrorMessage() != null) {
                message += " (" + e.getOriginalErrorMessage() + ")";
            } else
                message += " (" + e.getMessage() + ")";
            throw new Exception(message);
        }
    }

    public void offline() throws Exception {
        ReplicatorProperties params = new ReplicatorProperties();
        GoOfflineEvent goOfflineEvent = new GoOfflineEvent(params);

        try {
            handleEventSynchronous(goOfflineEvent);
        } catch (Exception e) {
            logger.error("Offline operation failed", e);
            throw new Exception("Offline operation failed: " + e.toString());
        }
    }

    public void offlineDeferred(Map<String, String> controlParams) throws Exception {
        ReplicatorProperties params = new ReplicatorProperties(controlParams);
        DeferredOfflineEvent deferredOfflineEvent = new DeferredOfflineEvent(params);

        try {
            handleEventSynchronous(deferredOfflineEvent);
        } catch (Exception e) {
            logger.error("Online operation failed", e);
            throw new Exception("Online operation failed: " + e.toString());
        }
    }

    public String flush(long timeout) throws Exception {
        try {
            FlushEvent flushEvent = new FlushEvent();
            handleEventSynchronous(flushEvent);

            return flushEvent.eventId;
        } catch (Exception e) {
            logger.error("Flush operation failed", e);
            throw new Exception("Flush operation failed: " + e.getMessage());
        }
    }

    public boolean waitForState(String stateName, long timeout) throws Exception {
        State desiredState = stmap.getStateByName(stateName);
        if (desiredState == null) {
            throw new Exception("Unknown state name: " + stateName);
        }
        if (timeout == 0)
            timeout = 1800;
        else if (timeout < 0 || timeout > 1800)
            throw new Exception("Limit must be between 0 and 1800 seconds: " + timeout);

        if (logger.isDebugEnabled())
            logger.debug("Waiting for state: state=" + desiredState + " seconds=" + timeout);

        StateTransitionLatch latch = sm.createStateTransitionLatch(desiredState, true);
        State finalState = null;
        Future<State> result = adminThreadPool.submit(latch);
        try {
            finalState = result.get(timeout, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            if (logger.isDebugEnabled())
                logger.debug("Timed out waiting for state: " + stateName);
            return false;
        }

        if (latch.isExpected() || desiredState.equals(finalState)) {
            if (logger.isDebugEnabled())
                logger.debug("Wait operation concluded successfully; found expected state: " + stateName);
            return true;
        } else if (latch.isError()) {
            String message = "Replicator failed and is in error state: " + finalState.getName();
            if (logger.isDebugEnabled())
                logger.debug(message);
            throw new Exception(message);
        } else {
            String message = "Replication reached unexpected state: " + finalState.getName();
            if (logger.isDebugEnabled())
                logger.debug(message);
            throw new Exception(message);
        }
    }

    public boolean waitForAppliedSequenceNumber(String seqno, long timeout) throws Exception {
        try {
            boolean success = openReplicator.waitForAppliedEvent(seqno, timeout);
            return success;
        } catch (Exception e) {
            logger.error("Wait operation failed", e);
            throw new Exception("Wait operation failed: " + e.getMessage(), e);
        }
    }

    protected String assertPropertyDefault(String key, String value) {
        if (properties.getString(key) == null) {
            logger.info("Assigning default global property value: key=" + key + " default value=" + value);
            properties.setString(key, value);
        }
        return properties.getString(key);
    }

    protected String assertPropertySet(String key) throws ReplicatorException {
        String value = properties.getString(key);
        if (value == null)
            throw new ReplicatorException("Required property not set: key=" + key);
        else
            return value;
    }

    protected OpenReplicatorPlugin loadAndConfigurePlugin(String prefix, String name) throws ReplicatorException {
        String pluginPrefix = prefix + "." + name.trim();

        // Find the plug-in class name.
        String rawClassName = properties.getString(pluginPrefix);
        if (rawClassName == null)
            throw new ReplicatorException("Plugin class name property is missing or null:  key=" + pluginPrefix);
        String pluginClassName = rawClassName.trim();
        logger.info("Loading plugin: key=" + pluginPrefix + " class name=" + pluginClassName);

        // Subset plug-in properties.
        ReplicatorProperties pluginProperties = properties.subset(pluginPrefix + ".", true);
        if (logger.isDebugEnabled())
            logger.debug("Plugin properties: " + pluginProperties.toString());

        // Load the plug-in class and configure its properties.
        OpenReplicatorPlugin plugin;
        try {
            plugin = (OpenReplicatorPlugin) Class.forName(pluginClassName).newInstance();
            pluginProperties.applyProperties(plugin);
        } catch (PropertyException e) {
            throw new ReplicatorException("Unable to configure plugin properties: key=" + pluginPrefix + " class name=" + pluginClassName + " : " + e.getMessage(), e);
        } catch (InstantiationException e) {
            throw new ReplicatorException("Unable to load plugin class: key=" + pluginPrefix + " class name=" + pluginClassName, e);
        } catch (IllegalAccessException e) {
            throw new ReplicatorException("Unable to load plugin class: key=" + pluginPrefix + " class name=" + pluginClassName, e);
        } catch (ClassNotFoundException e) {
            throw new ReplicatorException("Unable to load plugin class: key=" + pluginPrefix + " class name=" + pluginClassName, e);
        }

        // Plug-in is ready to go, so prepare it and call configure.
        try {
            plugin.prepare(this);
        } catch (ReplicatorException e) {
            throw new ReplicatorException("Unable to configure plugin: key=" + pluginPrefix + " class name=" + pluginClassName, e);
        } catch (Throwable t) {
            String message = "Unable to configure plugin: key=" + pluginPrefix + " class name=" + pluginClassName;
            logger.error(message, t);
            throw new ReplicatorException(message, t);
        }

        // It worked. We have a configured plugin.
        logger.info("Plug-in configured successfully: key=" + pluginPrefix + " class name=" + pluginClassName);
        return plugin;
    }

    protected void doConfigure() throws ReplicatorException {
        // Ensure auto-enable property is valid.
        assertPropertyDefault(ReplicatorConf.AUTO_ENABLE, ReplicatorConf.AUTO_ENABLE_DEFAULT);

        // Ensure source ID is available.
        assertPropertyDefault(ReplicatorConf.SOURCE_ID, ReplicatorConf.SOURCE_ID_DEFAULT);

        // Find and load open replicator plugin
        String replicatorName = assertPropertySet(ReplicatorConf.OPEN_REPLICATOR);
        if (openReplicator != null) {
            openReplicator.release();
        }
        openReplicator = loadAndConfigurePlugin(ReplicatorConf.OPEN_REPLICATOR, replicatorName);

        // Call configure method.
        openReplicator.configure(properties);
    }

    public void configure(ReplicatorProperties tp) throws Exception {
        handleEventSynchronous(new ConfigureEvent(tp));
    }

    private void handleEventSynchronous(Event event) throws ReplicatorException {
        EventRequest request = null;
        try {
            request = eventDispatcher.put(event);
            request.get();
        } catch (InterruptedException e) {
            logger.warn("Event processing was interrupted: " + event.getClass().getName());
            Thread.currentThread().interrupt();
            return;
        } catch (ExecutionException e) {
            logger.warn("Event processing failed: " + event.getClass().getName(), e);
            return;
        }

        Object annotation = request.getAnnotation();
        if (annotation instanceof ReplicatorException) {
            ReplicatorException e = (ReplicatorException) annotation;
            if (logger.isDebugEnabled())
                logger.debug("Event processing failed", e);
            throw e;
        }
    }

    private void loadProperties(Event event, Entity entity, Transition transition, int actionType) throws TransitionRollbackException {
        try {
            propertiesManager.loadProperties();
            properties = propertiesManager.getProperties();
        } catch (ReplicatorException e) {
            if (logger.isDebugEnabled())
                logger.debug("Unable to load properties", e);
            throw new TransitionRollbackException("Unable to load properties file: " + e.getMessage(), event, entity, transition, actionType, e);
        }
    }

    public CountDownLatch getDoneLatch() {
        return doneLatch;
    }

    public void setDoneLatch(CountDownLatch doneLatch) {
        this.doneLatch = doneLatch;
    }

    public List<String> getExtensionNames() {
    	ReplicatorRuntime runtime = this.openReplicator.getReplicatorRuntime();
    	if (runtime == null) {
			return new ArrayList<String>();
		}
        return runtime.getExtensionNames();
    }

    public static ReplicatorProperties getConfigurationProperties(String serviceName) throws ReplicatorException {
        ReplicatorRuntimeConf runtimeConf = ReplicatorRuntimeConf.getConfiguration(serviceName);
        PropertiesManager propertiesManager = new PropertiesManager(runtimeConf.getReplicatorProperties());
        propertiesManager.loadProperties();
        return propertiesManager.getProperties();
    }
}
