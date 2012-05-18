/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.data;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LoadDataFileQuery extends StatementData {
    private static final long serialVersionUID = 1L;

    private int fileId;
    private int filenameStartPos;

    private int filenameEndPos;

    private static final String LOAD_DATA_FILENAME = "((LOW_PRIORITY|CONCURRENT)\\s+)?(LOCAL\\s+)?INFILE\\s+(\\'.*\\')\\s+((REPLACE|IGNORE)\\s+)?INTO";

    private static final Pattern LOAD_DATA_FILENAME_PATTERN = Pattern.compile(LOAD_DATA_FILENAME, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    public LoadDataFileQuery(String queryString, long time, String defaultDb, int fileId, int startingPos, int endingPos) {
        super(queryString, time, defaultDb);
        this.fileId = fileId;
        this.filenameStartPos = startingPos;
        this.filenameEndPos = endingPos;
    }

    public int getFileID() {
        return fileId;
    }

    public void setLocalFile(File temporaryFile) {
        String query = this.getQuery();

        StringBuffer strBuf = new StringBuffer(query.substring(0, filenameStartPos));

        String fileName = getFileName();
        Matcher matcher = LOAD_DATA_FILENAME_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            strBuf.append(" ");
            strBuf.append(fileName.replace(matcher.group(4), "'" + temporaryFile.getPath() + "'"));
            strBuf.append(" ");
        }

        strBuf.append(query.substring(filenameEndPos));
        this.setQuery(strBuf.toString());
    }

    public boolean isLocal() {
        Matcher matcher = LOAD_DATA_FILENAME_PATTERN.matcher(getFileName());
        if (matcher.matches()) {
            return matcher.group(3) != null;
        }
        return false;
    }

    private String getFileName() {
        return this.getQuery().substring(filenameStartPos, filenameEndPos).trim();
    }

    public int getFilenameStartPos() {
        return filenameStartPos;
    }

    public int getFilenameEndPos() {
        return filenameEndPos;
    }

}
