/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine.test;

import com.taobao.common.tedis.replicator.statemachine.Entity;
import com.taobao.common.tedis.replicator.statemachine.Event;
import com.taobao.common.tedis.replicator.statemachine.Guard;
import com.taobao.common.tedis.replicator.statemachine.State;

public class NegationGuard implements Guard {
	
	private final Guard guard;
	
	public NegationGuard(Guard guard) {
		this.guard = guard;
	}

	@Override
	public boolean accept(Event message, Entity entity, State state) {
		return (!guard.accept(message, entity, state));
	}

}
