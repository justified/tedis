/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Implements a finite state machine. Finite state machines are a simple but
 * powerful formalism for describing state of applications, particularly those
 * with real-time or concurrent behavior. Finite state machines consist of the
 * following elements:
 * <ul>
 * <li>States - The set of legal states of the system</li>
 * <li>Transitions - The legal changes between states</li>
 * <li>Events - A set of messages that may induce state changes</li>
 * <li>Guards - A set of conditions that determine the conditions under which an
 * event causes a particular transition to be taken</li>
 * </ul>
 * Usage of state machines is quite simple. Programs create a state machine from
 * a StateMachineMap, which is a static definition. The state machine then
 * accepts events and takes appropriate transitions depending on the state
 * machine definition.
 * <p>
 * The following example shows how to build a state machine and deliver events.
 * <p>
 *
 * <pre>
 * <code>
 *  // Build the state transition map.
 *  StateTransitionMap map = new StateTransitionMap();
 *  State started = new State("STARTED", null, StateType.START);
 *  State aborted = new State("ABORTED", null, StateType.END);
 *  State committed = new State("COMMITTED", null, StateType.END);
 *  Transition do_abort = new Transition(started, new PositiveGuard(), abort);
 *  Transition do_commit = new Transition(started, new NegativeGuard(), commit);
 *
 *  map.addState(started);
 *  map.addState(aborted);
 *  map.addState(committed);
 *  map.addTransition(do_abort);
 *  map.addTransition(do_commit);
 *  map.build();
 *
 *  Create a state machine and deliver an event.
 *  StateMachine sm = new StateMachine(map);
 *  sm.deliverEvent(new Event("abort"));
 * </code>
 * </pre>
 *
 * State machine maps can include Action classes, which allow clients to define
 * procedures that execute when an event triggers a transition, an state is
 * entered, or a state is exited.
 * <p>
 * State machines enforce basic synchronization between threads by synchronizing
 * the applyEvent() call. Additional synchronization, if required, must be
 * supplied by the application.
 * <p>
 * Finally, state machines have an error handling model that includes a family
 * of exceptions to signal error conditions both large and small. There is also
 * a default error state that will
 * <p>
 */
public class StateMachine {
    private static Logger logger = Logger.getLogger(StateMachine.class);
    private volatile State state;
    private final Entity entity;
    private final StateTransitionMap map;
    private int transitions = 0;
    private int maxTransitions = 0;
    private List<StateChangeListener> listeners = new ArrayList<StateChangeListener>();
    private boolean forwardChainEnabled = false;

    public StateMachine(StateTransitionMap map, Entity entity) {
        this.map = map;
        this.entity = entity;
        setState(map.getStartState());
    }

    public synchronized void setMaxTransitions(int max) {
        this.maxTransitions = max;
    }

    public synchronized void addListener(StateChangeListener listener) {
        listeners.add(listener);
    }

    public synchronized boolean removeListener(StateChangeListener listener) {
        return listeners.remove(listener);
    }

    public synchronized void applyEvent(Event event) throws FiniteStateException, InterruptedException {
        if (maxTransitions > 0) {
            transitions++;
            if (transitions > maxTransitions)
                throw new FiniteStateException("Max transition count exceeded: state=" + state.getName() + " transition count=" + transitions);
        }

        // Find the next transition. This is guaranteed to be non-null.
        Transition transition = map.nextTransition(state, event, entity);
        State nextState = transition.getOutput();
        if (logger.isDebugEnabled()) {
            logger.debug("Executing state transition: input state=" + state.getName() + " transition=" + transition.getName() + " output state=" + nextState.getName());
        }

        int actionType = -1;
        State prevState = state;
        try {
            // Compute the least common parent between the current and next
            // state. Entry and exit actions fire below this state only in
            // the state hierarchy.
            State leastCommonParent = state.getLeastCommonParent(nextState);

            // If we are transitioning to a new state look for exit actions.
            if (state != nextState) {
                // Fire exit actions up to the state below the least common parent
                // if it exists.
                State exitState = state;
                if (logger.isDebugEnabled())
                    logger.debug("Searching for exit actions for current state: " + state.getName());

                while (exitState != null && exitState != leastCommonParent) {
                    if (exitState.getExitAction() != null) {
                        Action exitAction = exitState.getExitAction();
                        actionType = Action.EXIT_ACTION;
                        if (logger.isDebugEnabled())
                            logger.debug("Executing exit action for state: " + exitState.getName());
                        exitAction.doAction(event, entity, transition, actionType);
                    }

                    exitState = exitState.getParent();
                }
            }

            // Fire transition action if it exists.
            if (transition.getAction() != null) {
                Action transitionAction = transition.getAction();
                actionType = Action.TRANSITION_ACTION;
                if (logger.isDebugEnabled())
                    logger.debug("Executing action for transition: " + transition.getName());
                transitionAction.doAction(event, entity, transition, actionType);
            }

            // Transition to the next state and look for entry actions.
            if (state != nextState) {
                if (logger.isDebugEnabled())
                    logger.debug("Entering new state: " + nextState.getName());

                if (logger.isDebugEnabled())
                    logger.debug("Searching for entry actions for next state: " + nextState.getName());

                // We now set the state to the next state.
                setState(nextState);

                // Fire entry actions from the state below the least common
                // parent (if there is one) to the next state itself.
                State[] entryStates = nextState.getHierarchy();
                int startIndex = -1;
                if (leastCommonParent == null)
                    startIndex = 0;
                else {
                    for (int i = 0; i < entryStates.length; i++) {
                        if (entryStates[i] == leastCommonParent) {
                            startIndex = i + 1;
                            break;
                        }
                    }
                }

                for (int i = startIndex; i < entryStates.length; i++) {
                    State entryState = entryStates[i];
                    if (entryState.getEntryAction() != null) {
                        Action entryAction = entryState.getEntryAction();
                        actionType = Action.ENTER_ACTION;
                        if (logger.isDebugEnabled())
                            logger.debug("Executing entry action for state: " + entryState.getName());
                        entryAction.doAction(event, entity, transition, actionType);
                    }
                }

                // Call listeners to let them know what has happened.
                for (StateChangeListener listener : listeners) {
                    listener.stateChanged(entity, prevState, state);
                }

                // Process chained state.
                if (isForwardChainEnabled()) {
                    // Now see if we have a chained transition to handle. We can
                    // expect to get
                    // a FiniteStateException since we may not have a chained
                    // transition.
                    try {
                        if ((transition = map.nextTransition(state, event, entity)) != null) {
                            applyEvent(event);
                        }
                    } catch (FiniteStateException f) {
                        // Just ignore it.
                        return;
                    }
                }

            }
        } catch (InterruptedException e) {
            // Interrupts are treated as rollbacks. This enables state
            // operations to be cancelled.
            if (prevState != state) {
                setState(prevState);
            }

            // Log and rethrow the interrupt exception.
            if (logger.isDebugEnabled())
                logger.debug("Transition interrupted and rolled back: state=" + state.getName() + " transition=" + transition.getName() + " actionType=" + actionType);
            throw e;
        } catch (TransitionRollbackException e) {
            // Roll back the state if necessary. This would happen if an
            // entry action rolled backup.
            if (prevState != state) {
                setState(prevState);
            }

            // Log and rethrow the rollback exception.
            if (logger.isDebugEnabled())
                logger.debug("Transition rolled back: state=" + state.getName() + " transition=" + transition.getName() + " actionType=" + actionType);
            throw e;
        } catch (TransitionFailureException e) {
            // Transition to the error state and rethrow the exception.
            if (logger.isDebugEnabled())
                logger.debug("Transition failed: state=" + state.getName() + " transition=" + transition.getName() + " actionType=" + actionType);

            State errorState = map.getErrorState();

            // Make sure we have an error state!
            if (errorState == null) {
                String msg = "Attempt to throw TransitionFailureException when no error state exists";
                logger.error(msg, e);
                throw new FiniteStateException(msg, e);
            }

            // Now transition to it or try to at least.
            try {
                Action errorStateEntryAction = errorState.getEntryAction();
                if (errorStateEntryAction != null) {
                    if (logger.isDebugEnabled()) {
                        if (logger.isDebugEnabled())
                            logger.debug("Executing entry action for error state: " + errorState.getName());
                    }
                    errorStateEntryAction.doAction(event, entity, transition, Action.ENTER_ACTION);
                }

                // Make the error state official and call listeners to inform
                // them.
                setState(errorState);
                for (StateChangeListener listener : listeners) {
                    listener.stateChanged(entity, prevState, state);
                }
            } catch (Throwable t) {
                // This bad. Nothing to do but throw an generic exception.
                throw new FiniteStateException("Transition to error state failed", t);
            }
            // Rethrow failure that the application sees there has been an
            // error.
            throw e;
        }
    }

    public State getState() {
        return state;
    }

    private synchronized void setState(State state) {
        this.state = state;
    }

    public Entity getEntity() {
        return entity;
    }

    public boolean isEndState() {
        return state.isEnd();
    }

    public boolean isForwardChainEnabled() {
        return forwardChainEnabled;
    }

    public void setForwardChainEnabled(boolean forwardChainEnabled) {
        this.forwardChainEnabled = forwardChainEnabled;
    }

    public State getErrorState() {
        return this.map.getErrorState();
    }

    public synchronized StateTransitionLatch createStateTransitionLatch(State expected, boolean exitOnError) {
        // Must be synchronized when instantiating latch.
        return new StateTransitionLatch(this, expected, exitOnError);
    }
}