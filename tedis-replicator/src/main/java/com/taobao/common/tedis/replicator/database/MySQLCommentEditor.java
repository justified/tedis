/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MySQLCommentEditor implements SqlCommentEditor {
    protected Pattern standardPattern;
    protected Pattern sprocPattern;
    protected SqlOperationMatcher sqlMatcher = new MySQLOperationMatcher();

    @Override
    public String addComment(String statement, SqlOperation sqlOp, String comment) {
        // Look for a stored procedure or function creation.
        if (sqlOp.getOperation() == SqlOperation.CREATE) {
            int objectType = sqlOp.getObjectType();
            if (objectType == SqlOperation.PROCEDURE || objectType == SqlOperation.FUNCTION) {
                // Processing for CREATE PROCEDURE/FUNCTION -- add a COMMENT.
                // Following regex splits on line boundaries.
                String[] lines = statement.split("(?m)$");
                StringBuffer sb = new StringBuffer();
                boolean hasComment = false;
                for (String line : lines) {
                    String uline = line.toUpperCase();
                    if (uline.indexOf("COMMENT") > -1) {
                        sb.append("    COMMENT '" + comment + "'");
                        hasComment = true;
                    } else if (uline.indexOf("BEGIN") > -1) {
                        if (!hasComment) {
                            sb.append("    COMMENT '" + comment + "'");
                            hasComment = true;
                        }
                        sb.append(line);
                    } else
                        sb.append(line);
                }
                return sb.toString();
            }
        }

        // For any others just append the comment.
        return statement + " /* " + comment + " */";
    }

    @Override
    public String formatAppendableComment(SqlOperation sqlOp, String comment) {
        // Look for a stored procedure or function and return null. They are not
        // safe for appending.
        if (sqlOp.getOperation() == SqlOperation.CREATE) {
            int objectType = sqlOp.getObjectType();
            if (objectType == SqlOperation.PROCEDURE || objectType == SqlOperation.FUNCTION) {
                return null;
            }
        }

        // For any others return a properly formatted comment.
        return " /* " + comment + " */";
    }

    @Override
    public String fetchComment(String statement, SqlOperation sqlOp) {
        // Select correct comment pattern.
        Pattern commentPattern = standardPattern;
        if (sqlOp.getOperation() == SqlOperation.CREATE) {
            int objectType = sqlOp.getObjectType();
            if (objectType == SqlOperation.PROCEDURE)
                commentPattern = sprocPattern;
            else if (objectType == SqlOperation.FUNCTION)
                commentPattern = sprocPattern;
        }

        // Look for pattern match and return value if found.
        Matcher m = commentPattern.matcher(statement);
        if (m.find())
            return m.group(1);
        else
            return null;
    }

    @Override
    public void setCommentRegex(String regex) {
        String standardRegex = "\\/\\* (" + regex + ") \\*\\/";
        String sprocRegex = "COMMENT\\s*'(" + regex + ").*'";
        standardPattern = Pattern.compile(standardRegex);
        sprocPattern = Pattern.compile(sprocRegex);
    }
}