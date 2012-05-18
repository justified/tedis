/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.applier;

import java.io.Serializable;

public class ApplierStats implements Serializable {
    private static final long serialVersionUID = -217060098298976388L;
    long applied = 0;
    long firstAppliedTime = -1;
    long lastAppliedTime = -1;

    public ApplierStats() {

    }

    public ApplierStats(ApplierStats stats) {
        applied = stats.applied;
        firstAppliedTime = stats.firstAppliedTime;
        lastAppliedTime = stats.lastAppliedTime;
    }

    public void updateApplied() {
        long time = new java.util.Date().getTime();
        if (firstAppliedTime == -1)
            firstAppliedTime = time;
        lastAppliedTime = time;
        applied++;
    }

    public long getApplied() {
        return applied;
    }

    public double getSeconds() {
        return 1.e-3 * (lastAppliedTime - firstAppliedTime);
    }

    double getAppliedPerSec() {
        if (lastAppliedTime == firstAppliedTime)
            return 0.;
        return applied / getSeconds();
    }

    public String toString() {
        return "applied " + getApplied() + " in " + getSeconds() + " secs " + getAppliedPerSec() + "/sec";
    }

}
