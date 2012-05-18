/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.conf;

public class ReplicatorConf {
    /** Applier name. */
    static public final String OPEN_REPLICATOR = "replicator.plugin";

    /** Whether we are operating normally or in slave takeover mode. */
    static public final String NATIVE_SLAVE_TAKEOVER = "replicator.nativeSlaveTakeover";
    static public final String NATIVE_SLAVE_TAKEOVER_DEFAULT = "false";

    /** Whether to go online automatically at startup time. */
    static public final String AUTO_ENABLE = "replicator.auto_enable";
    static public final String AUTO_ENABLE_DEFAULT = "false";

    /** Whether to automatically provision this server at startup time. */
    static public final String AUTO_PROVISION = "replicator.auto_provision";
    static public final String AUTO_PROVISION_DEFAULT = "false";

    /** Source Identifier for THL and ReplDBMSEvents */
    static public final String SOURCE_ID = "replicator.source_id";
    static public final String SOURCE_ID_DEFAULT = "tedis";

    /** Service name to which replication service belongs */
    static public final String SERVICE_NAME = "service.name";

    /** Where Replicator stores metadata */
    static public final String METADATA_SCHEMA = "replicator.schema";
    static public final String METADATA_SCHEMA_DEFAULT = "tedis_default";

    /** Whether to log slave updates. */
    static public final String LOG_SLAVE_UPDATES = "replicator.log.slave.updates";
    static public final String LOG_SLAVE_UPDATES_DEFAULT = "false";

    /** Extension parameter names. */
    static public final String EXTENSIONS = "replicator.extensions";
    static public final String EXTENSION_ROOT = "replicator.extension";

    /** Pipeline and stage parameter names. */
    static public final String PIPELINE = "replicator.pipeline";
    static public final String PIPELINE_ROOT = "replicator.pipeline";
    static public final String STAGE_ROOT = "replicator.stage";

    /** Applier parameter names. */
    static public final String APPLIER = "applier";
    static public final String APPLIER_ROOT = "replicator.applier";

    /** Extractor parameter names. */
    static public final String EXTRACTOR = "extractor";
    static public final String EXTRACTOR_ROOT = "replicator.extractor";

    /** Prefix for filter property definitions. */
    static public final String FILTER = "filter";
    static public final String FILTERS = "filters";
    static public final String FILTER_ROOT = "replicator.filter";

    /** Store parameter names. */
    static public final String STORE = "store";
    static public final String STORE_ROOT = "replicator.store";

    /** Applier failure policy */
    static public final String APPLIER_FAILURE_POLICY = "replicator.applier.failure_policy";
    static public final String APPLIER_FAILURE_POLICY_DEFAULT = "stop";

    /** Extractor failure policy (stop|skip) */
    static public final String EXTRACTOR_FAILURE_POLICY = "replicator.extractor.failure_policy";
    static public final String EXTRACTOR_FAILURE_POLICY_DEFAULT = "stop";

    /** Should consistency check be sensitive to column names (true|false) */
    static public final String APPLIER_CONSISTENCY_COL_NAMES = "replicator.applier.consistency_column_names";
    static public final String APPLIER_CONSISTENCY_COL_NAMES_DEFAULT = "true";

    /**
     * This information will be used by the sql router to create data sources
     * dynamically
     */
    static public final String RESOURCE_JDBC_URL = "replicator.resourceJdbcUrl";
    /** Default value provided to enable unit tests to run. */
    static public final String RESOURCE_JDBC_URL_DEFAULT = "jdbc:mysql://localhost/${DBNAME}";
    static public final String RESOURCE_JDBC_DRIVER = "replicator.resourceJdbcDriver";
    static public final String RESOURCE_PRECEDENCE = "replicator.resourcePrecedence";
    static public final String RESOURCE_PRECEDENCE_DEFAULT = "99";
    static public final String RESOURCE_VENDOR = "replicator.resourceVendor";
    static public final String RESOURCE_LOGDIR = "replicator.resourceLogDir";
    static public final String RESOURCE_LOGPATTERN = "replicator.resourceLogPattern";
    static public final String RESOURCE_DISKLOGDIR = "replicator.resourceDiskLogDir";
    static public final String RESOURCE_PORT = "replicator.resourcePort";
    static public final String RESOURCE_DATASERVER_HOST = "replicator.resourceDataServerHost";

    static public final String GLOBAL_DB_USER = "replicator.global.db.user";
    static public final String GLOBAL_DB_PASSWORD = "replicator.global.db.password";

}
