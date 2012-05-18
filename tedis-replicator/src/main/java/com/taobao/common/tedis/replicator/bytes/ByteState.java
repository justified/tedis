/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.bytes;

public enum ByteState {
    /** No current state. */
    NONE,
    /** Current bytes are accepted. */
    ACCEPTED,
    /** Processing a binary string. */
    BUFFERING,
    /** Current byte is a accepted and is an escaped character. */
    ESCAPE
}