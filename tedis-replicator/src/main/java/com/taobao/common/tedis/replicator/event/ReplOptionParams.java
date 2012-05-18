/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.event;

public class ReplOptionParams {

    /**
     * Service that originally generated this event.
     */
    public static String SERVICE = "service";

    /**
     * If set, contains the name of a shard to which this event is assigned.
     */
    public static String SHARD_ID = "shard";

    /**
     * The default shard ID if no other ID can be assigned.
     */
    public static String SHARD_ID_UNKNOWN = "#UNKNOWN";

    /**
     * Prefix for internal options.
     */
    public static String INTERNAL_OPTIONS_PREFIX = "##";

    /**
     * Java character set name of byte-encoded strings. This is a statement
     * option.
     */
    public static String JAVA_CHARSET_NAME = "##charset";

    /**
     * ServerId. This is a statement option.
     */
    public static String SERVER_ID = "mysql_server_id";

    /**
     * Contains value "true" if the transaction is unsafe for bi-directional
     * replication.
     */
    public static String BIDI_UNSAFE = "bidi_unsafe";

    /**
     * Indicates whether this transaction needs to rollback. No value is
     * required. If this is defined, then the transaction should rollback.
     */
    public static final String ROLLBACK = "rollback";

    /**
     * Indicates whether this transaction is unsafe for block commit. As above,
     * no value is required. If this property is defined, then the transaction
     * is unsafe. If not defined, it is safe for block commit.
     */
    public static final String UNSAFE_FOR_BLOCK_COMMIT = "unsafe_for_block_commit";
}