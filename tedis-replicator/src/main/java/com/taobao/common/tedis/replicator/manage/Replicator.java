/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.manage;

public class Replicator {
    public static final String SERVICE_NAME = "serviceName";

    public static final String SIMPLE_SERVICE_NAME = "simpleServiceName";

    public static final String NODE_DATA_ID = "tedis-replicator-node";

    public static final String NODE_REGISTERNAME = "tedis-replicator-node-name";

    public static final String NODE_GROUPNAME = "tedis-replicator-node-group-name";

    public static final String APPLIED_LAST_SEQNO = "appliedLastSeqno";

    public static final String APPLIED_LAST_EVENT_ID = "appliedLastEventId";

    public static final String APPLIED_LATENCY = "appliedLatency";

    public static final String LATEST_EPOCH_NUMBER = "latestEpochNumber";

    public static final String CURRENT_EVENT_ID = "currentEventId";

    public static final String HOST = "host";

    public static final String UPTIME_SECONDS = "uptimeSeconds";

    public static final String TIME_IN_STATE_SECONDS = "timeInStateSeconds";

    public static final String CURRENT_TIME_MILLIS = "currentTimeMillis";

    public static final String STATE = "state";

    public static final String SOURCEID = "sourceId";

    public static final String USER = "user";

    public static final String PASSWORD = "password";

    public static final String MAX_PORT = "maxPort";

    public static final String PENDING_EXCEPTION_MESSAGE = "pendingExceptionMessage";

    public static final String PENDING_ERROR_CODE = "pendingErrorCode";

    public static final String PENDING_ERROR = "pendingError";

    public static final String PENDING_ERROR_SEQNO = "pendingErrorSeqno";

    public static final String PENDING_ERROR_EVENTID = "pendingErrorEventId";

    public static final String OFFLINE_REQUESTS = "offlineRequests";

    static public final String RESOURCE_JDBC_URL = "resourceJdbcUrl";

    static public final String RESOURCE_JDBC_DRIVER = "resourceJdbcDriver";

    static public final String RESOURCE_DATASERVER_HOST = "resourceDataServerHost";

    public static final String RESOURCE_PRECEDENCE = "resourcePrecedence";

    static public final String RESOURCE_VENDOR = "resourceVendor";

    static public final String RESOURCE_LOGDIR = "resourceLogDir";

    static public final String RESOURCE_DISK_LOGDIR = "resourceDiskLogDir";

    static public final String RESOURCE_LOGPATTERN = "resourceLogPattern";

    static public final String RESOURCE_PORT = "resourcePort";

    public static final String SEQNO_TYPE = "seqnoType";

    public static final String SEQNO_TYPE_LONG = "java.lang.Long";

    public static final String SEQNO_TYPE_STRING = "java.lang.String";

    public static final String EXTENSIONS = "extensions";

    public static final long DEFAULT_LATEST_EPOCH_NUMBER = -1;
    public static final String DEFAULT_LAST_EVENT_ID = "0:0";
}
