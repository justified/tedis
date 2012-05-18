/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator;

public class ReplicatorException extends Exception {
    private static final long serialVersionUID = 3323334831416977490L;
    private String originalErrorMessage = null;
    private String extraData = null;

    public ReplicatorException() {
        super();
    }

    public ReplicatorException(String arg0) {
        super(arg0);
    }

    public ReplicatorException(Throwable arg0) {
        super(arg0);
        if (arg0 instanceof ReplicatorException) {
            ReplicatorException exc = (ReplicatorException) arg0;
            this.extraData = exc.extraData;
            this.originalErrorMessage = exc.originalErrorMessage;
        }
    }

    public ReplicatorException(String arg0, Throwable arg1) {
        super(arg0, arg1);
        if (arg1 instanceof ReplicatorException) {
            ReplicatorException exc = (ReplicatorException) arg1;
            this.extraData = exc.extraData;
            this.originalErrorMessage = exc.originalErrorMessage;
        } else
            this.originalErrorMessage = arg0;
    }

    public void setOriginalErrorMessage(String originalErrorMessage) {
        this.originalErrorMessage = originalErrorMessage;
    }

    public String getOriginalErrorMessage() {
        return originalErrorMessage;
    }

    public String getExtraData() {
        return extraData;
    }

    public void setExtraData(String extraData) {
        this.extraData = extraData;
    }
}
