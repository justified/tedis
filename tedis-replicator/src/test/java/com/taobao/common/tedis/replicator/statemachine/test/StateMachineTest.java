/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import com.taobao.common.tedis.replicator.statemachine.Action;
import com.taobao.common.tedis.replicator.statemachine.Entity;
import com.taobao.common.tedis.replicator.statemachine.EntityAdapter;
import com.taobao.common.tedis.replicator.statemachine.Event;
import com.taobao.common.tedis.replicator.statemachine.EventTypeGuard;
import com.taobao.common.tedis.replicator.statemachine.FiniteStateException;
import com.taobao.common.tedis.replicator.statemachine.Guard;
import com.taobao.common.tedis.replicator.statemachine.PositiveGuard;
import com.taobao.common.tedis.replicator.statemachine.RegexGuard;
import com.taobao.common.tedis.replicator.statemachine.State;
import com.taobao.common.tedis.replicator.statemachine.StateMachine;
import com.taobao.common.tedis.replicator.statemachine.StateTransitionLatch;
import com.taobao.common.tedis.replicator.statemachine.StateTransitionMap;
import com.taobao.common.tedis.replicator.statemachine.StateType;
import com.taobao.common.tedis.replicator.statemachine.Transition;
import com.taobao.common.tedis.replicator.statemachine.TransitionFailureException;
import com.taobao.common.tedis.replicator.statemachine.TransitionNotFoundException;
import com.taobao.common.tedis.replicator.statemachine.TransitionRollbackException;
import com.taobao.common.tedis.replicator.statemachine.event.test.StringEvent;

public class StateMachineTest extends TestCase {
	public StateMachineTest(String name) {
		super(name);
	}

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * State machine map throws a FiniteStateException if you attempt to build a
	 * state machine with no states.
	 */
	public void testNoEmpty() throws Exception {
		StateTransitionMap map = new StateTransitionMap();
		try {
			map.build();
			throw new Exception("Able to build empty state machine map");
		} catch (FiniteStateException e) {
			// OK.
		}
	}

	/**
	 * State machine map throws a FiniteStateException if you attempt to build a
	 * state machine with no start state.
	 */
	public void testNoStartState() throws Exception {
		StateTransitionMap map = new StateTransitionMap();
		map.addState(new State("END", StateType.END));
		try {
			map.build();
			throw new Exception("Able to build state machine with no start state");
		} catch (FiniteStateException e) {
			// OK.
		}
	}

	/**
	 * State machine map throws a FiniteStateException if you attempt to build a
	 * state machine with no end state(s).
	 */
	public void testNoEndState() throws Exception {
		StateTransitionMap map = new StateTransitionMap();
		map.addState(new State("START", StateType.START));
		try {
			map.build();
			throw new Exception("Able to build state machine with no end state(s)");
		} catch (FiniteStateException e) {
			// OK.
		}
	}

	/**
	 * Confirm that a state machine throws a FiniteStateException if there
	 * exists a non-start state with no inbound transition.
	 */
	public void testNoTransitionIn() throws Exception {
		// Define states and transitions.
		State start = new State("START", StateType.START);
		State middle1 = new State("MIDDLE1", StateType.ACTIVE);
		State middle2 = new State("MIDDLE2", StateType.ACTIVE);
		State end1 = new State("END1", StateType.END);
		State end2 = new State("END2", StateType.END);
		Transition s_m1 = new Transition(new PositiveGuard(), start, null, middle1);
		Transition s_m2 = new Transition(new PositiveGuard(), start, null, middle2);
		Transition m1_e1 = new Transition(new PositiveGuard(), middle1, null, end1);
		Transition m1_e2 = new Transition(new PositiveGuard(), middle1, null, end2);
		Transition m2_e2 = new Transition(new PositiveGuard(), middle2, null, end2);

		// Try with no transition into a middle node.
		StateTransitionMap map1 = new StateTransitionMap();
		map1.addState(start);
		map1.addState(middle1);
		map1.addState(middle2);
		map1.addState(end1);
		map1.addState(end2);
		map1.addTransition(s_m1);
		map1.addTransition(m1_e1);
		map1.addTransition(m2_e2);
		try {
			map1.build();
			throw new Exception("Able to build with no transition into middle node");
		} catch (FiniteStateException e) {
			// OK.
		}

		// Add the missing transition and confirm we now can build.
		map1.addTransition(s_m2);
		map1.build();

		// Try with no transition into a end node.
		StateTransitionMap map2 = new StateTransitionMap();
		map2.addState(start);
		map2.addState(middle1);
		map2.addState(middle2);
		map2.addState(end1);
		map2.addState(end2);
		map2.addTransition(s_m1);
		map2.addTransition(s_m2);
		map2.addTransition(m1_e2);
		map2.addTransition(m2_e2);
		try {
			map2.build();
			throw new Exception("Able to build with no transition into end node");
		} catch (FiniteStateException e) {
			// OK.
		}

		// Add the missing transition and confirm we now can build.
		map2.addTransition(m1_e1);
		map2.build();
	}

	/**
	 * Confirm that a state machine throws a FiniteStateException if there
	 * exists a non-end state with no outbound transition.
	 */
	public void testNoTransitionOut() throws Exception {
		// Define states and transitions.
		State start = new State("START", StateType.START);
		State middle1 = new State("MIDDLE1", StateType.ACTIVE);
		State middle2 = new State("MIDDLE2", StateType.ACTIVE);
		State end1 = new State("END1", StateType.END);
		State end2 = new State("END2", StateType.END);
		Transition s_m1 = new Transition(new PositiveGuard(), start, null, middle1);
		Transition s_m2 = new Transition(new PositiveGuard(), start, null, middle2);
		Transition m1_m2 = new Transition(new PositiveGuard(), middle1, null, middle2);
		Transition m2_m1 = new Transition(new PositiveGuard(), middle2, null, middle1);
		Transition m1_e1 = new Transition(new PositiveGuard(), middle1, null, end1);
		Transition m1_e2 = new Transition(new PositiveGuard(), middle1, null, end2);
		Transition m2_e2 = new Transition(new PositiveGuard(), middle2, null, end2);

		// Try with no transition into a middle node.
		StateTransitionMap map1 = new StateTransitionMap();
		map1.addState(start);
		map1.addState(middle1);
		map1.addState(middle2);
		map1.addState(end1);
		map1.addState(end2);
		map1.addTransition(m1_m2);
		map1.addTransition(m2_m1);
		map1.addTransition(m1_e1);
		map1.addTransition(m2_e2);
		try {
			map1.build();
			throw new Exception("Able to build with no transition out of start node");
		} catch (FiniteStateException e) {
			// OK.
		}

		// Add the missing transition and confirm we now can build.
		map1.addTransition(s_m1);
		map1.build();

		// Try with no transition out of a middle node.
		StateTransitionMap map2 = new StateTransitionMap();
		map2.addState(start);
		map2.addState(middle1);
		map2.addState(middle2);
		map2.addState(end1);
		map2.addState(end2);
		map2.addTransition(s_m1);
		map2.addTransition(s_m2);
		map2.addTransition(m1_e1);
		map2.addTransition(m1_e2);
		try {
			map2.build();
			throw new Exception("Able to build with no transition out of middle node");
		} catch (FiniteStateException e) {
			// OK.
		}

		// Add the missing transition and confirm we now can build.
		map2.addTransition(m2_e2);
		map2.build();
	}

	/**
	 * Confirm that a state machine with one start and one end state and a
	 * default transition is accepted and moves into the end state.
	 */
	public void testMinimumMap() throws Exception {
		Action nullAction = new Action() {
			public void doAction(Event ev, Entity e, Transition t, int actionType) {
			}
		};

		// Construct and build the map.
		StateTransitionMap map = new StateTransitionMap();
		State start = new State("START", StateType.START, nullAction, nullAction);
		State end = new State("END", StateType.END, nullAction, nullAction);
		Transition transition = new Transition("START-TO-END", new PositiveGuard(), start, nullAction, end);

		map.addState(start);
		map.addState(end);
		map.addTransition(transition);
		map.build();

		// Start a state machine and test state.
		StateMachine sm = new StateMachine(map, new EntityAdapter(null));
		SampleListener listener = new SampleListener();
		sm.addListener(listener);
		assertEquals("Expect initial state", start, sm.getState());
		assertEquals("Not end state", false, sm.isEndState());

		// Send a message and confirm the state.
		sm.applyEvent(new Event(null));
		assertEquals("Expect end state", end, sm.getState());
		assertEquals("Is end state", true, sm.isEndState());
		assertEquals("Listener calls", 1, listener.getChanges());
	}

	/**
	 * Confirm that a state machine with alternate paths chooses only the path
	 * that matches.
	 */
	public void testAlternatePaths() throws Exception {
		// Construct and build the map. There is only one possible
		// transition from start to end2.
		StateTransitionMap map = new StateTransitionMap();
		State start = new State("START", StateType.START);
		State end1 = new State("END1", StateType.END);
		State end2 = new State("END1", StateType.END);
		Transition t1 = new Transition("t1", new NegationGuard(new PositiveGuard()), start, null, end1);
		Transition t2 = new Transition("t2", new NegationGuard(new PositiveGuard()), start, null, end2);
		Transition t3 = new Transition("t3", new PositiveGuard(), start, null, end2);

		map.addState(start);
		map.addState(end1);
		map.addState(end2);
		map.addTransition(t1);
		map.addTransition(t2);
		map.addTransition(t3);
		map.build();

		// Start a state machine and test state.
		StateMachine sm = new StateMachine(map, new EntityAdapter(null));
		assertEquals("Expect initial state", start, sm.getState());
		assertEquals("Not end state", false, sm.isEndState());

		// Send a message and confirm the state goes to end2.
		sm.applyEvent(new Event(null));
		assertEquals("Expect end state", end2, sm.getState());
		assertEquals("Is end state", true, sm.isEndState());
	}

	/**
	 * Confirm that a state machine throws a TransitionNotFoundException if it
	 * enters a state and cannot find a matching transition when a message is
	 * processed.
	 */
	public void testNoTransitionFound() throws Exception {
		// Construct and build the map.
		StateTransitionMap map = new StateTransitionMap();
		State start = new State("START", StateType.START);
		State end = new State("END", StateType.END);
		Transition transition = new Transition(new NegationGuard(new PositiveGuard()), start, null, end);

		map.addState(start);
		map.addState(end);
		map.addTransition(transition);
		map.build();

		// Start a state machine and test state.
		StateMachine sm = new StateMachine(map, new EntityAdapter(null));

		// Send a message and confirm we get an exception.
		try {
			sm.applyEvent(new Event(null));
			throw new Exception("Did not generate an exception when there were no matching transitions");
		} catch (TransitionNotFoundException e) {
			// OK
		}
	}

	/**
	 * Test handling of successful actions, actions that fail with a rollback,
	 * and actions that fail with a run-time exception.
	 */
	public void testActionFailure() throws Exception {
		// Construct and build the map.
		StateTransitionMap map = new StateTransitionMap();
		SampleAction exitAction = new SampleAction();
		SampleAction transitionAction = new SampleAction();
		SampleAction entryAction = new SampleAction();
		SampleAction[] actions = { exitAction, transitionAction, entryAction };

		State start = new State("START", StateType.START, null, exitAction);
		State end = new State("END", StateType.END, entryAction, null);
		Transition transition = new Transition("START-TO-END", new PositiveGuard(), start, transitionAction, end);

		map.addState(start);
		map.addState(end);
		map.addTransition(transition);
		map.build();

		// Test that we reach the end state when all actions succeed.
		StateMachine sm1 = new StateMachine(map, new EntityAdapter(null));
		sm1.applyEvent(new Event(null));
		assertEquals("Reached end state", "END", sm1.getState().getName());

		// Test that we can roll back from any of the actions.
		EntityAdapter ea = new EntityAdapter(null);
		StateMachine sm2 = new StateMachine(map, ea);
		for (int i = 0; i < actions.length; i++) {
			// Reset actions.
			for (SampleAction sa : actions)
				sa.setSucceed();

			// Pick one to roll back.
			actions[i].setRollback();
			Event event = new Event(null);

			try {
				sm2.applyEvent(event);
				throw new Exception("Event failed to generate rollback: iteration=" + i);
			} catch (TransitionRollbackException e) {
				assertEquals("Correct event, iteration=" + i, event, e.getEvent());
				assertEquals("Correct entity, iteration=" + i, ea, e.getEntity());
				assertEquals("Correct transition, iteration=" + i, transition, e.getTransition());
			}
			assertEquals("Still at start state", "START", sm2.getState().getName());
		}

		// Show that an unhandled exception is passed up through the stack.
		for (int i = 0; i < actions.length; i++) {
			// Reset actions.
			for (SampleAction sa : actions)
				sa.setSucceed();

			// Pick one to fail.
			actions[i].setBug();

			// Create state machine and an event.
			EntityAdapter ea3 = new EntityAdapter(null);
			Event event = new Event(null);
			StateMachine sm3 = new StateMachine(map, ea3);

			try {
				sm3.applyEvent(event);
				throw new Exception("Event failed to generate failure: iteration=" + i);
			} catch (Throwable e) {
			}
		}
	}

	/**
	 * Confirm that if there are two alternate transitions, the first path added
	 * is taken.
	 */
	public void testFirstAlternateTransition() throws Exception {
		// Construct and build the map. There is only one possible
		// transition from start to end2.
		State start = new State("START", StateType.START);
		State end1 = new State("END1", StateType.END);
		State end2 = new State("END2", StateType.END);
		Transition t1 = new Transition(new PositiveGuard(), start, null, end1);
		Transition t2 = new Transition(new PositiveGuard(), start, null, end2);

		StateTransitionMap map1 = new StateTransitionMap();
		map1.addState(start);
		map1.addState(end1);
		map1.addState(end2);
		map1.addTransition(t1);
		map1.addTransition(t2);
		map1.build();

		StateTransitionMap map2 = new StateTransitionMap();
		map2.addState(start);
		map2.addState(end1);
		map2.addState(end2);
		map2.addTransition(t2);
		map2.addTransition(t1);
		map2.build();

		// Confirm map1 goes to end1.
		StateMachine sm1 = new StateMachine(map1, new EntityAdapter(null));
		sm1.applyEvent(new Event(null));
		assertEquals("Expect end state", end1, sm1.getState());
		assertEquals("Is end state", true, sm1.isEndState());

		// Confirm map2 goes to end2.
		StateMachine sm2 = new StateMachine(map2, new EntityAdapter(null));
		sm2.applyEvent(new Event(null));
		assertEquals("Expect end state", end2, sm2.getState());
		assertEquals("Is end state", true, sm2.isEndState());
	}
	
	public void testContinusTransition() throws Exception {
		State start = new State("START", StateType.START);
		State midle = new State("MIDDLE", StateType.ACTIVE);
		State end = new State("END", StateType.END);
		Transition t1 = new Transition(new PositiveGuard(), start, null, midle);
		Transition t2 = new Transition(new PositiveGuard(), midle, null, end);

		StateTransitionMap map1 = new StateTransitionMap();
		map1.addState(start);
		map1.addState(midle);
		map1.addState(end);
		map1.addTransition(t1);
		map1.addTransition(t2);
		map1.build();
		
		StateMachine sm = new StateMachine(map1, new EntityAdapter(null));
		sm.applyEvent(new Event(null));
		assertEquals("Expect middle state", midle, sm.getState());
	}

	/**
	 * Confirm that we throw a FiniteStateException if the maxTransitions count
	 * is non-zero and we attempt more than that number of transitions.
	 */
	public void testMaxTransitionCount() throws Exception {
		// Construct and build the map. There is only one possible
		// transition from start to end2.
		State start = new State("START", StateType.START);
		State middle1 = new State("M1", StateType.ACTIVE);
		State middle2 = new State("M2", StateType.ACTIVE);
		State end = new State("END1", StateType.END);
		Transition s_m1 = new Transition("s_m1", new PositiveGuard(), start, null, middle1);
		Transition m1_m2 = new Transition("m1_m2", new PositiveGuard(), middle1, null, middle2);
		Transition m2_m1 = new Transition("m2_m1", new PositiveGuard(), middle2, null, middle1);
		Transition m2_e = new Transition("m2_e", new PositiveGuard(), middle2, null, end);

		StateTransitionMap map1 = new StateTransitionMap();
		map1.addState(start);
		map1.addState(end);
		map1.addState(middle1);
		map1.addState(middle2);
		map1.addTransition(s_m1);
		map1.addTransition(m1_m2);
		map1.addTransition(m2_m1);
		map1.addTransition(m2_e);
		map1.build();

		// Create a new state machine that permits three transitions.
		StateMachine sm1 = new StateMachine(map1, new EntityAdapter(null));
		sm1.setMaxTransitions(3);

		// Confirm that we can apply a messsage three times and get a
		// FiniteStateException on the fourth try.
		sm1.applyEvent(new Event(null));
		assertEquals("Expect midle1 state", middle1, sm1.getState());

		sm1.applyEvent(new Event(null));
		assertEquals("Expect middle2 state", middle2, sm1.getState());

		sm1.applyEvent(new Event(null));
		assertEquals("Expect middle1 state", middle1, sm1.getState());

		try {
			sm1.applyEvent(new Event(null));
			throw new Exception("Able to exceed maxTransitions count!");
		} catch (FiniteStateException e) {
			// OK
		}
	}

	/**
	 * Confirm that we can use an updated entity and guards to implement a loop
	 * that terminates after 10 iterations.
	 */
	public void testLoopImplementation() throws Exception {
		Integer entity = new Integer(0);

		// Create an action to increment the entity each time we enter a state.
		Action incrementorAction = new Action() {
			public void doAction(Event ev, Entity entity, Transition transition, int actionType) {
				EntityAdapter ea = (EntityAdapter) entity;
				int current = ((Integer) ea.getEntity()).intValue();
				Integer next = new Integer(current + 1);
				ea.setEntity(next);
			}
		};

		// Create a guard that returns true if the entity count is equal to or
		// greater than 10.
		Guard numberGuard = new Guard() {
			public boolean accept(Event message, Entity entity, State state) {
				EntityAdapter ea = (EntityAdapter) entity;
				int current = ((Integer) ea.getEntity()).intValue();
				return (current >= 10);
			}
		};

		// Construct and build the map. There is only one possible
		// transition from start to end2.
		State start = new State("START", StateType.START);
		State end = new State("END1", StateType.END);
		Transition s_2_s = new Transition("START-START", new NegationGuard(numberGuard), start, incrementorAction, start);
		Transition s_2_e = new Transition("START-END", numberGuard, start, null, end);

		StateTransitionMap map1 = new StateTransitionMap();
		map1.addState(start);
		map1.addState(end);
		map1.addTransition(s_2_s);
		map1.addTransition(s_2_e);
		map1.build();

		// Create a new state machine that permits three transitions.
		StateMachine sm1 = new StateMachine(map1, new EntityAdapter(entity));
		SampleListener listener = new SampleListener();
		sm1.setMaxTransitions(11);
		sm1.addListener(listener);

		// Continue to apply events until we reach the end state.
		int counter = 0;
		while (!sm1.getState().isEnd()) {
			sm1.applyEvent(new Event(null));
			counter++;
		}

		// Assert that we looped through 11 times.
		assertEquals("Events applied 11 times", 11, counter);

		// Assert the entity counter is 10 as well.
		Integer finalInteger = (Integer) ((EntityAdapter) sm1.getEntity()).getEntity();
		int finalInt = finalInteger.intValue();
		assertEquals("Entity counter is 10", 10, finalInt);

		// Assert that there was one state transition recorded by the
		// listener.
		assertEquals("State changes", 1, listener.getChanges());
	}

	/**
	 * Confirm that we can use NamedEvent and RegexGuard to rout based on events
	 * containing strings. This test loops until an event containing the string
	 * END1 is received.
	 */
	public void testRegexGuard() throws Exception {
		// Construct and build the map. There is only one possible
		// transition from start to end2.
		State start = new State("START", StateType.START);
		State end = new State("END1", StateType.END);
		Transition s_2_e = new Transition(new RegexGuard("END1"), start, null, end);
		Transition s_2_s = new Transition(new RegexGuard(".*"), start, null, start);

		// Create map. Loading order is important since we need to see the
		// END1 guard first in order to take that path.
		StateTransitionMap map1 = new StateTransitionMap();
		map1.addState(start);
		map1.addState(end);
		map1.addTransition(s_2_e);
		map1.addTransition(s_2_s);
		map1.build();

		// Create a new state machine.
		StateMachine sm1 = new StateMachine(map1, new EntityAdapter(new Object()));

		// Continue to apply events until we reach the end state.
		int counter = 0;
		String[] eventNames = { "END", "END0", "END1" };
		while (!sm1.getState().isEnd()) {
			Event e = new StringEvent(eventNames[counter]);
			sm1.applyEvent(e);
			counter++;
		}

		// Assert that we looped through 3 times.
		assertEquals("Events applied 3 times", 3, counter);
	}

	/**
	 * Confirm that InstanceOfGuard accepts only events that are a type of the
	 * supplied class.
	 */
	public void testInstanceOfGuard() throws Exception {
		EventTypeGuard acceptAny = new EventTypeGuard(Event.class);
		EventTypeGuard acceptDummyEvent = new EventTypeGuard(SampleEvent.class);

		// If type is an Event, we accept any event.
		assertTrue("Accept all", acceptAny.accept(new Event(null), null, null));
		assertTrue("Accept all", acceptAny.accept(new SampleEvent(), null, null));

		// If type is DummyEvent we only expect that.
		assertFalse("Accept all", acceptDummyEvent.accept(new Event(null), null, null));
		assertTrue("Accept all", acceptDummyEvent.accept(new SampleEvent(), null, null));
	}

	/**
	 * Confirm that sub-state transitions have the following properties.
	 * <ol>
	 * <li>In-going transitions connect directly to sub-states.
	 * <li>Out-going transitions may go from sub-state or from any enclosing
	 * state
	 * </ol>
	 */
	public void testSubstateTransitions() throws Exception {
		// Build a state machine map that nests as follows:
		// START (no nested sub-states)
		// MAIN ->
		// SUB1
		// SUB2 ->
		// INNER1
		// INNER2
		// STOP (no nested sub-states)

		// Create map with states and transitions.
		StateTransitionMap map1 = new StateTransitionMap();
		State start = map1.addState("START", StateType.START, null);
		State main = map1.addState("MAIN", StateType.ACTIVE, null);
		State sub1 = map1.addState("SUB1", StateType.ACTIVE, main);
		State sub2 = map1.addState("SUB2", StateType.ACTIVE, main);
		State inner1 = map1.addState("INNER1", StateType.ACTIVE, sub2);
		State inner2 = map1.addState("INNER2", StateType.ACTIVE, sub2);
		State end = map1.addState("END", StateType.END, null);

		map1.addTransition("START-MAIN", "START-MAIN", start, null, main);
		map1.addTransition("START-SUB1", "START-SUB1", start, null, sub1);
		map1.addTransition("START-SUB2", "START-SUB2", start, null, sub2);
		map1.addTransition("START-INNER1", "START-INNER1", start, null, inner1);
		map1.addTransition("START-INNER2", "START-INNER2", start, null, inner2);
		map1.addTransition("MAIN-START", "MAIN-START", main, null, start);
		map1.addTransition("SUB1-START", "SUB1-START", sub1, null, start);
		map1.addTransition("SUB2-START", "SUB2-START", sub2, null, start);
		map1.addTransition("INNER1-START", "INNER1-START", inner1, null, start);
		map1.addTransition("INNER2-START", "INNER2-START", inner2, null, start);
		map1.addTransition("MAIN-END", "MAIN-END", main, null, end);

		map1.build();

		// Create a new state machine.
		StateMachine sm1 = new StateMachine(map1, new EntityAdapter(new Object()));
		assertEquals("Starting state: start", start, sm1.getState());

		// Test every combination of moving into and out of sub-states by
		// "boxing"
		// the available transitions.
		State[] hierarchy = inner2.getHierarchy();
		for (int inner = 0; inner < hierarchy.length; inner++) {
			for (int outer = 0; outer <= inner; outer++) {
				// Must be in starting state.
				assertEquals("Starting state: start", start, sm1.getState());

				// Transition from start to inner state.
				State innerState = hierarchy[inner];
				String message1 = "START-" + innerState.getBaseName();
				sm1.applyEvent(new StringEvent(message1));
				assertEquals("State after delivering " + message1 + " to START", innerState, sm1.getState());

				// Transition from start to START using outer transition.
				State outerState = hierarchy[outer];
				String message2 = outerState.getBaseName() + "-START";
				// System.out.println("Current state=" + sm1.getState() +
				// " message=" + message2);
				sm1.applyEvent(new StringEvent(message2));
				assertEquals("State after delivering " + message1 + " to " + innerState, start, sm1.getState());
			}
		}

		// Test moving from start to main and back.
		sm1.applyEvent(new StringEvent("START-MAIN"));
		assertEquals("Move to main", main, sm1.getState());
		sm1.applyEvent(new StringEvent("MAIN-START"));
		assertEquals("Move to main", start, sm1.getState());

		// Test moving from start to sub1 and back.
		sm1.applyEvent(new StringEvent("START-MAIN"));
		assertEquals("Move to main", main, sm1.getState());
		sm1.applyEvent(new StringEvent("MAIN-START"));
		assertEquals("Move to main", start, sm1.getState());
	}

	/**
	 * Confirm state methods for sub-state relationships work correctly. This
	 * test case works by constructing a simply hierarchy with related and
	 * unrelated states in order to test predicates and relations.
	 */
	public void testSubstateRelations() throws Exception {
		State top = new State("top", StateType.ACTIVE);
		State middle = new State("middle", StateType.ACTIVE, top);
		State child1 = new State("child1", StateType.ACTIVE, middle);
		State child2 = new State("child2", StateType.ACTIVE, middle);
		State noRelation = new State("noRelation", StateType.ACTIVE);

		// Test predicates etc on top state.
		assertFalse("top has no enclosing state", top.isSubstate());
		assertFalse("top unrelated to other state", top.isSubstateOf(middle));
		assertNull("top has no parent", top.getParent());
		assertNull("top has no least common parent with unrelated state", top.getLeastCommonParent(noRelation));
		assertEquals("top is least common parent of middle", top, top.getLeastCommonParent(middle));
		assertEquals("top is least common parent of child2", top, top.getLeastCommonParent(child2));

		// Test predicates etc on middle state.
		assertTrue("middle is substate", middle.isSubstate());
		assertFalse("middle not enclosed by child1", middle.isSubstateOf(child1));
		assertTrue("middle is substate of top", middle.isSubstateOf(top));
		assertEquals("middle has parent", top, middle.getParent());
		assertEquals("top is least common parent of middle", top, middle.getLeastCommonParent(top));
		assertEquals("middle is least common parent with child1", middle, middle.getLeastCommonParent(child1));
		assertNull("middle has no least common parent with unrelated state", middle.getLeastCommonParent(noRelation));

		// Test predicates etc on child1 state.
		assertTrue("child1 is substate", child1.isSubstate());
		assertFalse("child1 not enclosed by child2", child1.isSubstateOf(child2));
		assertTrue("child1 is substate of top", child1.isSubstateOf(top));
		assertEquals("child1 has parent", middle, child1.getParent());
		assertEquals("middle is least common parent from child1", middle, child1.getLeastCommonParent(middle));
		assertEquals("middle is least common parent from child2", middle, child1.getLeastCommonParent(child2));
		assertEquals("top is least common parent with from child2", top, middle.getLeastCommonParent(top));
	}

	/**
	 * Confirm that sub-state entry actions conform to the following rules.
	 * <ol>
	 * <li>Entry action: When you transition into a sub-state from another
	 * state, any entry actions for enclosing states around the sub-state also
	 * execute starting with the outermost enclosing state you are entering and
	 * continuing to the sub-state entry action.</li>
	 * <li>Exit action: When you transition from a substate into another state,
	 * exit actions for the sub-state and enclosing states fire starting with
	 * the sub-state and proceeding to the outermost enclosing state from which
	 * you are transitioning.</li>
	 * </ol>
	 */
	public void testSubstateActions() throws Exception {
		// Build a state machine map that nests as follows:
		// START (no nested sub-states)
		// MAIN ->
		// SUB1
		// SUB2 ->
		// INNER1
		// INNER2
		// STOP (no nested sub-states)

		// Define actions.
		SampleAction mainEntry = new SampleAction();
		SampleAction mainExit = new SampleAction();
		SampleAction sub1Entry = new SampleAction();
		SampleAction sub1Exit = new SampleAction();
		SampleAction sub2Entry = new SampleAction();
		SampleAction sub2Exit = new SampleAction();
		SampleAction inner1Entry = new SampleAction();
		SampleAction inner1Exit = new SampleAction();
		SampleAction inner2Entry = new SampleAction();
		SampleAction inner2Exit = new SampleAction();

		SampleAction[] actionArray = new SampleAction[] { mainEntry, mainExit, sub1Entry, sub1Exit, sub2Entry, sub2Exit, inner1Entry, inner1Exit, inner2Entry, inner2Exit };

		// Create map with states and transitions.
		StateTransitionMap map1 = new StateTransitionMap();
		State start = map1.addState("START", StateType.START, null);
		State main = map1.addState("MAIN", StateType.ACTIVE, null, mainEntry, mainExit);
		State sub1 = map1.addState("SUB1", StateType.ACTIVE, main, sub1Entry, sub1Exit);
		State sub2 = map1.addState("SUB2", StateType.ACTIVE, main, sub2Entry, sub2Exit);
		State inner1 = map1.addState("INNER1", StateType.ACTIVE, sub2, inner1Entry, inner1Exit);
		State inner2 = map1.addState("INNER2", StateType.ACTIVE, sub2, inner2Entry, inner2Exit);
		State end = map1.addState("END", StateType.END, null);

		map1.addTransition("START-MAIN", "START-MAIN", start, null, main);
		map1.addTransition("START-SUB1", "START-SUB1", start, null, sub1);
		map1.addTransition("SUB1-INNER2", "SUB1-INNER2", sub1, null, inner2);
		map1.addTransition("INNER2-INNER1", "INNER2-INNER1", inner2, null, inner1);
		map1.addTransition("INNER1-SUB2", "INNER1-SUB2", inner1, null, sub2);
		map1.addTransition("SUB2-SUB2", "SUB2-SUB2", sub2, null, sub2);
		map1.addTransition("MAIN-END", "MAIN-END", main, null, end);

		map1.build();

		// Create a new state machine.
		StateMachine sm1 = new StateMachine(map1, new EntityAdapter(new Object()));
		assertEquals("Starting state: start", start, sm1.getState());

		// On transition from start to sub1, main and sub1 entry should fire.
		clearActionArray(actionArray);
		mainEntry.setSucceed();
		sub1Entry.setSucceed();
		sm1.applyEvent(new StringEvent("START-SUB1"));
		assertEquals("current state: sub1", sub1, sm1.getState());
		assertEquals("main entry fired", 1, mainEntry.getCount());
		assertEquals("sub1 entry fired", 1, sub1Entry.getCount());

		// On transition from sub1 to inner2, sub1 exit and sub2 + inner2
		// entry should fire.
		clearActionArray(actionArray);
		sub1Exit.setSucceed();
		sub2Entry.setSucceed();
		inner2Entry.setSucceed();
		sm1.applyEvent(new StringEvent("SUB1-INNER2"));
		assertEquals("current state: inner2", inner2, sm1.getState());
		assertEquals("sub1 exit fired", 1, sub1Exit.getCount());
		assertEquals("sub2 entry fired", 1, sub2Entry.getCount());
		assertEquals("inner2 entry fired", 1, inner2Entry.getCount());

		// On transition from inner2 to inner1, inner2 exit and inner1
		// entry should fire.
		clearActionArray(actionArray);
		inner2Exit.setSucceed();
		inner1Entry.setSucceed();
		sm1.applyEvent(new StringEvent("INNER2-INNER1"));
		assertEquals("current state: inner1", inner1, sm1.getState());
		assertEquals("inner2 eixt fired", 1, inner2Exit.getCount());
		assertEquals("inner1 entry fired", 1, inner1Entry.getCount());

		// On transition from from inner1 to sub2, inner1 exit fires.
		clearActionArray(actionArray);
		inner1Exit.setSucceed();
		sm1.applyEvent(new StringEvent("INNER1-SUB2"));
		assertEquals("current state: sub2", sub2, sm1.getState());
		assertEquals("inner1 exit fired", 1, inner1Exit.getCount());

		// On transition from from sub2 to sub2 no entry/exit actions
		// should fire.
		clearActionArray(actionArray);
		sm1.applyEvent(new StringEvent("SUB2-SUB2"));
		assertEquals("current state: sub2", sub2, sm1.getState());

		// On transition from from sub2 to end, sub2 + main exits
		// fire.
		clearActionArray(actionArray);
		sub2Exit.setSucceed();
		mainExit.setSucceed();
		sm1.applyEvent(new StringEvent("MAIN-END"));
		assertEquals("current state: end", end, sm1.getState());
		assertEquals("sub2 exit fired", 1, sub2Exit.getCount());
		assertEquals("main exit fired", 1, mainExit.getCount());
	}

	/**
	 * Confirm that a state machine will not accept an error state not already
	 * in the map and that a transition failure is illagal if there is no
	 * designated error state.
	 */
	public void testInvalidErrorStates() throws Exception {
		// Construct and build the map.
		StateTransitionMap map = new StateTransitionMap();
		SampleAction sampleAction = new SampleAction();
		State start = map.addState("START", StateType.START, null);
		State end = map.addState("END", StateType.END, null);
		map.addTransition("START-TO-END", new PositiveGuard(), start, sampleAction, end);
		// Try to add an invalid guard.
		try {
			map.setErrorState(new State("HMMM", StateType.ACTIVE));
			throw new Exception("Able to add new state as error state!");
		} catch (FiniteStateException e) {
		}

		map.build();

		// Start a state machine and test state.
		StateMachine sm = new StateMachine(map, new EntityAdapter(null));
		sampleAction.setFailure();

		try {
			sm.applyEvent(new Event(null));
			throw new Exception("Able to throw TransitionFailedException without error state");
		} catch (TransitionFailureException e) {
			throw new Exception("Unexpected transition failure exception returned!", e);
		} catch (FiniteStateException e) {
			// OK.
		}
	}

	/**
	 * Confirm that a state machine will transition into the error state firing
	 * the error state entry action if an action within a transition throws a
	 * TransitionFailureException.
	 */
	public void testErrorStateTransition() throws Exception {
		// Construct and build the map.
		StateTransitionMap map = new StateTransitionMap();
		SampleAction sampleAction = new SampleAction();
		SampleAction errorAction = new SampleAction();
		State start = map.addState("START", StateType.START, null);
		State test = map.addState("TEST", StateType.ACTIVE, null);
		State sub1 = map.addState("SUB1", StateType.ACTIVE, test);
		State sub2 = map.addState("SUB2", StateType.ACTIVE, test);
		State error = map.addState("ERROR", StateType.ACTIVE, null, errorAction, null);
		State end = map.addState("END", StateType.END, null);

		map.setErrorState(error);

		map.addTransition("START-TO-TEST", "START-TO-TEST", start, sampleAction, test);
		map.addTransition("TEST-TO-SUB1", "TEST-TO-SUB1", test, sampleAction, sub1);
		map.addTransition("SUB1-TO-SUB2", "SUB1-TO-SUB2", sub1, sampleAction, sub2);
		map.addTransition("SUB2-TO-SUB1", "SUB2-TO-SUB1", sub2, sampleAction, sub1);
		map.addTransition("ERROR-TO-TEST", "ERROR-TO-TEST", error, sampleAction, test);
		map.addTransition("TEST-TO-END", "TEST-TO-END", test, sampleAction, end);

		map.build();

		// Start a state machine.
		StateMachine sm = new StateMachine(map, new EntityAdapter(null));

		// Verify that we handle a failed transition into a top-level state.
		sampleAction.setFailure();
		try {
			sm.applyEvent(new StringEvent("START-TO-TEST"));
			throw new Exception("Failed transition did not throw TransitionFailedException");
		} catch (TransitionFailureException e) {
		}
		assertEquals("Checking for error state", "ERROR", sm.getState().getName());
		assertEquals("Checking error entry action fired", 1, errorAction.getCount());

		// Verify that we handle a failed transition into a sub-state.
		sampleAction.setSucceed();
		sm.applyEvent(new StringEvent("ERROR-TO-TEST"));
		assertEquals("Checking for test state", "TEST", sm.getState().getName());
		sampleAction.setFailure();
		try {
			sm.applyEvent(new StringEvent("TEST-TO-SUB1"));
			throw new Exception("Failed transition did not throw TransitionFailedException");
		} catch (TransitionFailureException e) {
		}
		assertEquals("Checking for error state", "ERROR", sm.getState().getName());
		assertEquals("Checking error entry action fired", 2, errorAction.getCount());

		// Verify that we handle a failed transition between sub-states.
		sampleAction.setSucceed();
		sm.applyEvent(new StringEvent("ERROR-TO-TEST"));
		sm.applyEvent(new StringEvent("TEST-TO-SUB1"));
		assertEquals("Checking for sub1 state", "TEST:SUB1", sm.getState().getName());
		sampleAction.setFailure();
		try {
			sm.applyEvent(new StringEvent("SUB1-TO-SUB2"));
			throw new Exception("Failed transition did not throw TransitionFailedException");
		} catch (TransitionFailureException e) {
		}
		assertEquals("Checking for error state", "ERROR", sm.getState().getName());
		assertEquals("Checking error entry action fired", 3, errorAction.getCount());

		// Verify we can still use the state machine.
		sampleAction.setSucceed();
		sm.applyEvent(new StringEvent("ERROR-TO-TEST"));
		sm.applyEvent(new StringEvent("TEST-TO-END"));
		assertEquals("Checking for end state", "END", sm.getState().getName());
	}

	/**
	 * Confirm that a state machine will build a state machine that states with
	 * no directly outbound or inbound transitions but whose substates have such
	 * transitions.
	 */
	public void testSuperstateWithoutTransitions() throws Exception {
		// Construct and build the map.
		StateTransitionMap map = new StateTransitionMap();
		SampleAction sampleAction = new SampleAction();
		State start = map.addState("START", StateType.START, null);
		State superstate = map.addState("SUPERSTATE", StateType.ACTIVE, null);
		State sub1 = map.addState("SUB1", StateType.ACTIVE, superstate);
		State sub2 = map.addState("SUB2", StateType.ACTIVE, superstate);
		State end = map.addState("END", StateType.END, null);

		map.addTransition("START-TO-SUB1", "START-TO-SUB1", start, sampleAction, sub1);
		map.addTransition("START-TO-SUB2", "START-TO-SUB2", start, sampleAction, sub2);
		map.addTransition("SUB1-TO-END", "SUB1-TO-END", sub1, sampleAction, end);
		map.addTransition("SUB2-TO-END", "SUB2-TO-END", sub2, sampleAction, end);

		map.build();
	}

	/**
	 * Confirm that a state machine will build a state machine having states
	 * with outbound transitions only through their parents.
	 */
	public void testSubStateWithOnlyParentTransitions() throws Exception {
		// Construct and build the map.
		StateTransitionMap map = new StateTransitionMap();
		SampleAction sampleAction = new SampleAction();
		State start = map.addState("START", StateType.START, null);
		State superstate = map.addState("SUPERSTATE", StateType.ACTIVE, null);
		State sub1 = map.addState("SUB1", StateType.ACTIVE, superstate);
		State sub2 = map.addState("SUB2", StateType.ACTIVE, superstate);
		State end = map.addState("END", StateType.END, null);

		map.setErrorState(sub2);

		map.addTransition("START-TO-SUB1", "START-TO-SUB1", start, sampleAction, sub1);
		map.addTransition("SUPERSTATE-TO-END", "SUPERSTATE-TO-END", sub1, sampleAction, end);

		map.build();
	}

	/**
	 * Test use of latches to confirm that the state machine has reached a
	 * particular state.
	 * 
	 * @throws Exception
	 */
	public void testLatches() throws Exception {
		// Create a state machine with multiple states and an error state.
		StateTransitionMap map = new StateTransitionMap();
		SampleAction sampleAction = new SampleAction();
		SampleAction errorAction = new SampleAction();
		State start = map.addState("START", StateType.START, null);
		State test = map.addState("TEST", StateType.ACTIVE, null);
		State error = map.addState("ERROR", StateType.ACTIVE, null, errorAction, null);
		State end = map.addState("END", StateType.END, null);

		map.setErrorState(error);

		map.addTransition("START-TO-TEST", "START-TO-TEST", start, sampleAction, test);
		map.addTransition("ERROR-TO-TEST", "ERROR-TO-TEST", error, sampleAction, test);
		map.addTransition("TEST-TO-END", "TEST-TO-END", test, sampleAction, end);

		map.build();

		// Start the state machine.
		StateMachine sm = new StateMachine(map, new EntityAdapter(null));
		ExecutorService exec = Executors.newSingleThreadExecutor();

		// Verify that we can wait for a transition into a desired state.
		StateTransitionLatch latch1 = sm.createStateTransitionLatch(test, false);
		Future<State> result1 = exec.submit(latch1);
		sm.applyEvent(new StringEvent("START-TO-TEST"));
		State s1 = result1.get();
		assertEquals("Checking normal state transition", test, s1);
		assertTrue("Latch should be done", latch1.isDone());
		assertTrue("Latch should have found desired state", latch1.isExpected());
		assertFalse("Latch should not have found error", latch1.isError());

		// Verify that we can wait for a transition into the error state by
		// forcing a failure. (Wait for end, but really look for error.)
		StateTransitionLatch latch2 = sm.createStateTransitionLatch(end, true);
		Future<State> result2 = exec.submit(latch2);
		sampleAction.setFailure();
		try {
			sm.applyEvent(new StringEvent("TEST-TO-END"));
			throw new Exception("Failed transition did not throw TransitionFailedException");
		} catch (TransitionFailureException e) {
		}

		State s2 = result2.get();
		assertEquals("Should be in error state", error, s2);
		assertTrue("Latch should be done", latch2.isDone());
		assertFalse("Latch should not have found desired state", latch2.isExpected());
		assertTrue("Latch should have found error", latch2.isError());

		// Verify that we time out if we don't reach our expected state.
		StateTransitionLatch latch3 = sm.createStateTransitionLatch(end, false);
		Future<State> result3 = exec.submit(latch3);
		sampleAction.setSucceed();
		sm.applyEvent(new StringEvent("ERROR-TO-TEST"));

		try {
			result3.get(3000, TimeUnit.MILLISECONDS);
			throw new Exception("Operation did not time out!");
		} catch (TimeoutException e) {
		}
		assertFalse("Latch should not be done", latch3.isDone());
		assertFalse("Latch should not have found desired state", latch3.isExpected());
		assertFalse("Latch should not have found error", latch3.isError());

		// Verify that we can cancel the latch after letting it wait for a
		// while.
		StateTransitionLatch latch4 = sm.createStateTransitionLatch(start, false);
		Future<State> result4 = exec.submit(latch3);
		Thread.sleep(500); // Bide a wee
		result4.cancel(true);
		assertFalse("Latch should not be done", latch4.isDone());
		assertFalse("Latch should not have found desired state", latch4.isExpected());
		assertFalse("Latch should not have found error", latch4.isError());
	}

	// Clear an action array making all actions illegal.
	private void clearActionArray(SampleAction[] actions) {
		for (int i = 0; i < actions.length; i++) {
			actions[i].clearCount();
			actions[i].setIllegal();
		}
	}
}