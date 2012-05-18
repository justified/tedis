/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine.event.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.statemachine.EntityAdapter;
import com.taobao.common.tedis.replicator.statemachine.Event;
import com.taobao.common.tedis.replicator.statemachine.RegexGuard;
import com.taobao.common.tedis.replicator.statemachine.State;
import com.taobao.common.tedis.replicator.statemachine.StateMachine;
import com.taobao.common.tedis.replicator.statemachine.StateTransitionMap;
import com.taobao.common.tedis.replicator.statemachine.StateType;
import com.taobao.common.tedis.replicator.statemachine.Transition;
import com.taobao.common.tedis.replicator.statemachine.TransitionNotFoundException;
import com.taobao.common.tedis.replicator.statemachine.TransitionRollbackException;
import com.taobao.common.tedis.replicator.statemachine.event.EventCompletionListener;
import com.taobao.common.tedis.replicator.statemachine.event.EventDispatcherTask;
import com.taobao.common.tedis.replicator.statemachine.event.EventRequest;
import com.taobao.common.tedis.replicator.statemachine.event.EventStatus;
import com.taobao.common.tedis.replicator.statemachine.test.SampleAction;

public class EventDispatcherTest extends TestCase {
	private static Logger logger = Logger.getLogger(EventDispatcherTest.class);

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * Verify that we can start and stop an event dispatcher.
	 */
	public void testStartStop() throws Exception {
		StateMachine sm = this.createSimpleStateMachine(null);
		EventDispatcherTask dispatcher = new EventDispatcherTask(sm);
		Assert.assertFalse("Should not be running yet", dispatcher.isRunning());
		dispatcher.start("testStartStop");
		Assert.assertTrue("should be running after start", dispatcher.isRunning());
		dispatcher.stop();
		Assert.assertFalse("Should not be running after stop", dispatcher.isRunning());
	}

	/**
	 * Verify that we submit and return status from successful state
	 * transitions.
	 */
	public void testEventSubmit() throws Exception {
		// Start the state machine and dispatcher.
		StateMachine sm = this.createSimpleStateMachine(null);
		EventDispatcherTask dispatcher = new EventDispatcherTask(sm);
		SampleStatusListener listener = new SampleStatusListener();
		dispatcher.setListener(listener);
		dispatcher.start("testEventSubmit");
		Assert.assertEquals("should be in start state", "START", sm.getState().getName());

		// Submit an event and confirm that we get expected status and also
		// update the state machine as expected.
		StringEvent e1 = new StringEvent("END");
		EventRequest futureStatus = dispatcher.put(e1);
		// EventStatus status = futureStatus.get(10, TimeUnit.SECONDS);
		EventStatus status = futureStatus.get();
		Assert.assertTrue(status.isSuccessful());
		Assert.assertFalse(status.isCancelled());
		Assert.assertNull(status.getException());
		Assert.assertEquals("should be in end state", "END", sm.getState().getName());

		// Stop the dispatcher.
		dispatcher.stop();
		Assert.assertFalse("Should not be running after stop", dispatcher.isRunning());

		// Ensure the listener got the same status as we got from the event.
		EventStatus status2 = listener.getStatusList().get(0);
		Assert.assertEquals("Checking eventstatus", status, status2);
	}

	/**
	 * Verify that we submit and return status as unsuccessful with an exception
	 */
	public void testNoTransition() throws Exception {
		// Start the state machine and dispatcher.
		StateMachine sm = this.createSimpleStateMachine(null);
		EventDispatcherTask dispatcher = new EventDispatcherTask(sm);
		SampleStatusListener listener = new SampleStatusListener();
		dispatcher.setListener(listener);
		dispatcher.start("testNoTransition");

		// Submit an event and confirm that we get expected status and also
		// update the state machine as expected.
		StringEvent e1 = new StringEvent("XXXXXXXX");
		EventRequest futureStatus = dispatcher.put(e1);
		EventStatus status = futureStatus.get();
		Assert.assertFalse(status.isSuccessful());
		Assert.assertFalse(status.isCancelled());
		Assert.assertTrue(status.getException() instanceof TransitionNotFoundException);

		// Stop the dispatcher.
		dispatcher.stop();
		Assert.assertFalse("Should not be running after stop", dispatcher.isRunning());

		// Ensure the listener got the same status as we got from the event.
		EventStatus status2 = listener.getStatusList().get(0);
		Assert.assertEquals("Checking eventstatus", status, status2);
	}

	/**
	 * Verify that we submit and return status as unsuccessful with an exception
	 * when a TransitionRollbackException is thrown.
	 */
	public void testTransitionRollback() throws Exception {
		// Start the state machine and dispatcher.
		StateMachine sm = this.createSimpleStateMachine(SampleAction.Outcome.ROLLBACK);
		EventDispatcherTask dispatcher = new EventDispatcherTask(sm);
		SampleStatusListener listener = new SampleStatusListener();
		dispatcher.setListener(listener);
		dispatcher.start("testTransitionRollback");

		// Submit an event and confirm that we get expected status and also
		// update the state machine as expected.
		StringEvent e1 = new StringEvent("START");
		EventRequest futureStatus = dispatcher.put(e1);
		EventStatus status = futureStatus.get();
		Assert.assertFalse(status.isSuccessful());
		Assert.assertFalse(status.isCancelled());
		Assert.assertTrue(status.getException() instanceof TransitionRollbackException);

		// Stop the dispatcher.
		dispatcher.stop();
		Assert.assertFalse("Should not be running after stop", dispatcher.isRunning());

		// Ensure the listener got the same status as we got from the event.
		EventStatus status2 = listener.getStatusList().get(0);
		Assert.assertEquals("Checking eventstatus", status, status2);
	}

	/**
	 * Verify that we can submit and process a large queue of events.
	 */
	public void testMultiEventSubmit() throws Exception {
		// Start the state machine and dispatcher.
		StateMachine sm = this.createTimedStateMachine(0, 0, 0);
		EventDispatcherTask dispatcher = new EventDispatcherTask(sm);
		dispatcher.start("testMultiEventSubmit");
		Assert.assertEquals("should be in start state", "START", sm.getState().getName());

		// Process 1000 transitions from START to MIDDLE and back again.
		for (int i = 0; i < 1000; i++) {
			logger.debug("iteration: " + i);
			// Transition to MIDDLE state.
			StringEvent e1 = new StringEvent("MIDDLE");
			EventRequest future1 = dispatcher.put(e1);
			EventStatus s1 = future1.get();
			Assert.assertEquals("Iteration [" + i + "] transition to MIDDLE", "MIDDLE", sm.getState().getName());
			Assert.assertTrue("Iteration [" + i + "] transition to MIDDLE (status)", s1.isSuccessful());

			// Transition back to START state.
			StringEvent e2 = new StringEvent("START");
			EventRequest future2 = dispatcher.put(e2);
			EventStatus s2 = future2.get();
			Assert.assertEquals("Iteration [" + i + "] transition to START", "START", sm.getState().getName());
			Assert.assertTrue("Iteration [" + i + "] transition to START (status)", s2.isSuccessful());
		}

		// Stop the dispatcher.
		dispatcher.stop();
		Assert.assertFalse("Should not be running after stop", dispatcher.isRunning());
	}

	/**
	 * Verify that we can halt a state machine that is blocked in a state entry
	 * action and restore the previous state by issuing a cancel request on the
	 * status future.
	 */
	public void testCancelEntryAction() throws Exception {
		// Start the state machine and dispatcher.
		StateMachine sm = this.createTimedStateMachine(Integer.MAX_VALUE, 0, 0);
		EventDispatcherTask dispatcher = new EventDispatcherTask(sm);
		dispatcher.start("testCancelEntryAction");
		Assert.assertEquals("should be in start state", "START", sm.getState().getName());

		// Submit an event and ensure we are blocked in the MIDDLE entry action.
		StringEvent e1 = new StringEvent("MIDDLE");
		Future<EventStatus> future1 = dispatcher.put(e1);
		try {
			EventStatus status = future1.get(1, TimeUnit.SECONDS);
			throw new Exception("Entry action block failed: success=" + status.isSuccessful());
		} catch (TimeoutException e) {
			logger.info("Blocked as expected in exit action");
		}

		// Try to cancel but don't allow the dispatcher to cancel a running
		// transition. This should ignore the attempt to cancel.
		boolean c1 = future1.cancel(false);
		Assert.assertFalse("task not cancelled", c1);
		try {
			EventStatus status = future1.get(1, TimeUnit.SECONDS);
			throw new Exception("Entry action block failed: success=" + status.isSuccessful());
		} catch (TimeoutException e) {
			logger.info("Still blocked as expected in exit action");
		}

		// Cancel again allowing the state machine to stop a running
		// transition.
		boolean c2 = future1.cancel(true);
		Assert.assertTrue("task is cancelled", c2);
		EventStatus status = future1.get(1, TimeUnit.SECONDS);
		Assert.assertFalse(status.isSuccessful());
		Assert.assertTrue(status.isCancelled());

		// Confirm we are back to the starting state and can transition to end.
		Assert.assertEquals("Should be in start state again", "START", sm.getState().getName());
		StringEvent e2 = new StringEvent("END");
		Future<EventStatus> future2 = dispatcher.put(e2);
		future2.get();
		Assert.assertEquals("should be in end state", "END", sm.getState().getName());

		// Stop the dispatcher.
		dispatcher.stop();
		Assert.assertFalse("Should not be running after stop", dispatcher.isRunning());
	}

	/**
	 * Verify that we can cancel a request that is still enqueued before it
	 * runs.
	 */
	public void testCancelNonRunning() throws Exception {
		// Start the state machine and dispatcher.
		StateMachine sm = this.createTimedStateMachine(Integer.MAX_VALUE, 0, 0);
		EventDispatcherTask dispatcher = new EventDispatcherTask(sm);
		dispatcher.start("testCancelNonRunning");

		// Submit an event and ensure we are blocked in the MIDDLE entry action.
		StringEvent e1 = new StringEvent("MIDDLE");
		Future<EventStatus> future1 = dispatcher.put(e1);
		try {
			EventStatus status = future1.get(1, TimeUnit.SECONDS);
			throw new Exception("Entry action block failed: success=" + status.isSuccessful());
		} catch (TimeoutException e) {
			logger.info("Blocked as expected in exit action");
		}

		// Submit another event to transition back.
		StringEvent e2 = new StringEvent("SILLY");
		Future<EventStatus> future2 = dispatcher.put(e2);

		// Cancel the new event.
		boolean c2 = future2.cancel(false);
		Assert.assertTrue("non-running task cancelled", c2);

		// Cancel the blocking event, then check status.
		future1.cancel(true);
		EventStatus status2 = future2.get(1, TimeUnit.SECONDS);
		Assert.assertFalse(status2.isSuccessful());
		Assert.assertTrue(status2.isCancelled());

		// Stop the dispatcher.
		dispatcher.stop();
		Assert.assertFalse("Should not be running after stop", dispatcher.isRunning());

		// Ensure the stuck event was also cancelled.
		EventStatus status1 = future1.get(2, TimeUnit.SECONDS);
		Assert.assertFalse("Pending event should have failed", status1.isSuccessful());
		Assert.assertTrue("Pending event should be cancelled", status1.isCancelled());
	}

	/**
	 * Verify that we can cancel a state machine that is blocked in a state
	 * entry action by delivering an out-of-band event that picks another state
	 * transition.
	 */
	public void testOutOfBand() throws Exception {
		// Start the state machine and dispatcher.
		StateMachine sm = this.createTimedStateMachine(Integer.MAX_VALUE, 0, 0);
		EventDispatcherTask dispatcher = new EventDispatcherTask(sm);
		dispatcher.start("testCancelEntryAction");
		Assert.assertEquals("should be in start state", "START", sm.getState().getName());

		// Submit an event and ensure we are blocked in the MIDDLE entry action.
		StringEvent e1 = new StringEvent("MIDDLE");
		Future<EventStatus> future1 = dispatcher.put(e1);
		try {
			EventStatus status = future1.get(2, TimeUnit.SECONDS);
			throw new Exception("Entry action block failed: success=" + status.isSuccessful());
		} catch (TimeoutException e) {
			logger.info("Blocked as expected in exit action");
		}

		// Submit an out-of-band event.
		StringEvent e2 = new StringEvent("END");
		Future<EventStatus> future2 = dispatcher.putOutOfBand(e2);

		// Prove that first event is cancelled and second is successful.
		EventStatus status1 = future1.get(2, TimeUnit.SECONDS);
		Assert.assertFalse(status1.isSuccessful());
		Assert.assertTrue(status1.isCancelled());

		EventStatus status2 = future2.get(2, TimeUnit.SECONDS);
		Assert.assertTrue(status2.isSuccessful());
		Assert.assertFalse(status2.isCancelled());
		Assert.assertNull(status2.getException());

		// Prove we reached the end state.
		Assert.assertEquals("should be in end state", "END", sm.getState().getName());

		// Stop the dispatcher.
		dispatcher.stop();
		Assert.assertFalse("Should not be running after stop", dispatcher.isRunning());
	}

	/**
	 * Verify that we correctly recognize an implicit out-of-band event that
	 * implements the OutOfBandInterface and cancel preceding events
	 * appropriately.
	 */
	public void testOutOfBandImplicit() throws Exception {
		// Start the state machine and dispatcher. This state machine will block
		// on transition to middle.
		StateMachine sm = this.createTimedStateMachine(0, Integer.MAX_VALUE, 0);
		EventDispatcherTask dispatcher = new EventDispatcherTask(sm);
		dispatcher.start("testOutOfBandImplicit");

		// Submit an event and ensure we are blocked in the MIDDLE entry action.
		StringEvent e1 = new StringEvent("MIDDLE");
		Future<EventStatus> future1 = dispatcher.put(e1);
		try {
			EventStatus status = future1.get(2, TimeUnit.SECONDS);
			throw new Exception("Entry action block failed: success=" + status.isSuccessful());
		} catch (TimeoutException e) {
			logger.info("Blocked as expected in exit action");
		}

		// Create and submit an implicit out-of-band event.
		StringEvent e2 = new OutOfBandStringEvent("END");
		Future<EventStatus> future2 = dispatcher.put(e2);

		// Prove that first event is cancelled and second is successful.
		EventStatus status1 = future1.get(2, TimeUnit.SECONDS);
		Assert.assertTrue(status1.isCancelled());

		EventStatus status2 = future2.get(2, TimeUnit.SECONDS);
		Assert.assertTrue(status2.isSuccessful());
		Assert.assertFalse(status2.isCancelled());
		Assert.assertNull(status2.getException());

		// Prove we reached the end state.
		Assert.assertEquals("should be in end state", "END", sm.getState().getName());

		// Stop the dispatcher.
		dispatcher.stop();
	}

	// Generate a simple state machine with start and end nodes. The start
	// to start transition can be set to generate bugs and the like.
	public StateMachine createSimpleStateMachine(SampleAction.Outcome outcome) throws Exception {
		// Construct and build the map. There is only one possible
		// transition from start to end.
		State start = new State("START", StateType.START);
		State end = new State("END", StateType.END);
		Transition s_2_e = new Transition(new RegexGuard("END"), start, null, end);
		SampleAction action = new SampleAction();
		if (outcome != null)
			action.setOutcome(outcome);
		Transition s_2_s = new Transition(new RegexGuard("START"), start, action, start);

		// Create map. Loading order is important since we need to see the
		// END guard first in order to take that path.
		StateTransitionMap map1 = new StateTransitionMap();
		map1.addState(start);
		map1.addState(end);
		map1.addTransition(s_2_e);
		map1.addTransition(s_2_s);
		map1.build();

		// Create a new state machine.
		StateMachine sm1 = new StateMachine(map1, new EntityAdapter(new Object()));
		return sm1;
	}

	// Generate a simple state machine with the following transitions:
	// START -> MIDDLE
	// MIDDLE -> START
	// START -> END
	// Only actions associate with the middle state will block. Here are
	// the affected actions.
	// * Entry action to MIDDLE
	// * Transition action to MIDDLE
	// * Exit action from MIDDLE
	public StateMachine createTimedStateMachine(int middleEntrySecs, int transitionToMiddleSecs, int middleExitSecs) throws Exception {
		// Start and end are simple states.
		State start = new State("START", StateType.START);
		State end = new State("END", StateType.END);

		// Middle has delays on entry and exit.
		SampleAction middleEntryAction = new SampleAction();
		middleEntryAction.setWaitSeconds(middleEntrySecs);
		SampleAction middleExitAction = new SampleAction();
		middleExitAction.setWaitSeconds(middleExitSecs);
		State middle = new State("MIDDLE", StateType.END, null, middleEntryAction, middleEntryAction);

		// Following transitions are immediate.
		Transition m_2_s = new Transition(new RegexGuard("START"), middle, null, start);
		Transition s_2_e = new Transition(new RegexGuard("END"), start, null, end);

		// Transition start to middle has a delay.
		SampleAction transitionAction = new SampleAction();
		transitionAction.setWaitSeconds(transitionToMiddleSecs);
		Transition s_2_m = new Transition(new RegexGuard("MIDDLE"), start, transitionAction, middle);

		// Create map. Loading order is important since we need to see the
		// END guard first in order to take that path.
		StateTransitionMap map1 = new StateTransitionMap();
		map1.addState(start);
		map1.addState(middle);
		map1.addState(end);
		map1.addTransition(s_2_e);
		map1.addTransition(s_2_m);
		map1.addTransition(m_2_s);
		map1.build();

		// Create a new state machine.
		StateMachine sm1 = new StateMachine(map1, new EntityAdapter(new Object()));
		return sm1;
	}
}

// Event status listener used for testing.
class SampleStatusListener implements EventCompletionListener {
	List<EventStatus> statusList = new ArrayList<EventStatus>(10);

	@Override
	public Object onCompletion(Event event, EventStatus status) throws InterruptedException {
		statusList.add(status);
		return null;
	}

	List<EventStatus> getStatusList() {
		return statusList;
	}
}