/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.manage;

public class ReplicatorParams {

    /** Set initial event ID when going online. */
    public static final String INIT_EVENT_ID = "extractFromId";

    /** Skip applying first N events after going online. */
    public static final String SKIP_APPLY_EVENTS = "skipApplyEvents";

    /** Stay online until sequence number has been processed. */
    public static final String ONLINE_TO_SEQNO = "toSeqno";

    /** Stay online until event ID has been processed. */
    public static final String ONLINE_TO_EVENT_ID = "toEventId";

    /** Stay online until source timestamp has been processed. */
    public static final String ONLINE_TO_TIMESTAMP = "toTimestamp";

    /** Stay online until next heartbeat has been processed. */
    public static final String ONLINE_TO_HEARTBEAT = "toHeartbeat";

    public static final String SKIP_APPLY_SEQNOS = "skipApplySeqnos";

    /** Go offline safely after next transactional boundary. */
    public static final String OFFLINE_TRANSACTIONAL = "atTransaction";

    /** Go offline after sequence number has been processed. */
    public static final String OFFLINE_AT_SEQNO = "atSeqno";

    /** Go offline after event ID has been processed. */
    public static final String OFFLINE_AT_EVENT_ID = "atEventId";

    /** Go offline after source timestamp has been processed. */
    public static final String OFFLINE_AT_TIMESTAMP = "atTimestamp";

}