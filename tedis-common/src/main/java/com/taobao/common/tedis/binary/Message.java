/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.binary;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 上午10:51:55
 * @version 1.0
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 425983229881691138L;
    private final byte[] channel;
    private final byte[] body;

    public Message(byte[] channel, byte[] body) {
        this.body = body;
        this.channel = channel;
    }

    public byte[] getChannel() {
        return (channel != null ? channel.clone() : null);
    }

    public byte[] getBody() {
        return (body != null ? body.clone() : null);
    }

    @Override
    public String toString() {
        return Arrays.toString(body);
    }

}
