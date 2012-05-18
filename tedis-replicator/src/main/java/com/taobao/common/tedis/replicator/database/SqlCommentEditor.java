/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

public interface SqlCommentEditor {
    public String addComment(String statement, SqlOperation sqlOperation, String comment);

    public String formatAppendableComment(SqlOperation sqlOperation, String comment);

    public void setCommentRegex(String regex);

    public String fetchComment(String baseStatement, SqlOperation sqlOperation);
}