/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine.test;

import com.taobao.common.tedis.replicator.statemachine.Action;
import com.taobao.common.tedis.replicator.statemachine.Entity;
import com.taobao.common.tedis.replicator.statemachine.Event;
import com.taobao.common.tedis.replicator.statemachine.Transition;
import com.taobao.common.tedis.replicator.statemachine.TransitionFailureException;
import com.taobao.common.tedis.replicator.statemachine.TransitionRollbackException;

public class SampleAction implements Action {
	public enum Outcome {
		SUCCEED, ROLLBACK, FAILURE, BUG, ILLEGAL
	};

	private Outcome outcome = Outcome.SUCCEED;
	private int wait = 0;
	private int count = 0;

	// Set the number of seconds to wait before proceeding to outcome.
	public void setWaitSeconds(int wait) {
		this.wait = wait;
	}

	// Set the outcome.
	public void setSucceed() {
		outcome = Outcome.SUCCEED;
	}

	public void setRollback() {
		outcome = Outcome.ROLLBACK;
	}

	public void setFailure() {
		outcome = Outcome.FAILURE;
	}

	public void setBug() {
		outcome = Outcome.BUG;
	}

	public void setIllegal() {
		outcome = Outcome.ILLEGAL;
	}

	public void setOutcome(Outcome outcome) {
		this.outcome = outcome;
	}

	public int getCount() {
		return count;
	}

	public void clearCount() {
		count = 0;
	}

	public void doAction(Event event, Entity entity, Transition transition, int actionType) throws TransitionRollbackException, TransitionFailureException, InterruptedException {
		// If we have a wait of more than 0, delay for that many seconds.
		if (wait > 0) {
			Thread.sleep(wait * 1000L);
		}

		// Process the outcome.
		if (outcome == Outcome.SUCCEED) {
			count++;
			return;
		} else if (outcome == Outcome.ROLLBACK)
			throw new TransitionRollbackException("rollback", event, entity, transition, actionType, null);
		else if (outcome == Outcome.FAILURE)
			throw new TransitionFailureException("rollback", event, entity, transition, actionType, null);
		else if (outcome == Outcome.BUG)
			throw new RuntimeException("fail");
		else if (outcome == Outcome.ILLEGAL)
			throw new RuntimeException("Illegal action call! Entity=" + entity + " Transition=" + transition);
	}
}
