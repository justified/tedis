/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;

public class BinlogIndex {
    private static Logger logger = Logger.getLogger(BinlogIndex.class);

    // binlog directory and base name
    private final File binlogDirectory;
    private final File indexFile;

    // List of files in index order.
    List<File> binlogFiles;

    public BinlogIndex(String directory, String baseName, boolean readIndex) throws ReplicatorException {
        this.binlogDirectory = new File(directory);
        if (!binlogDirectory.canRead()) {
            throw new MySQLExtractException("Binlog index missing or unreadable; check binlog directory and file pattern settings: " + binlogDirectory.getAbsolutePath());
        }

        indexFile = new File(binlogDirectory, baseName + ".index");
        if (!indexFile.canRead()) {
            throw new MySQLExtractException("Binlog index missing or unreadable; check binlog directory and file pattern settings: " + indexFile.getAbsolutePath());
        }

        if (readIndex)
            readIndex();
    }

    public void readIndex() throws ReplicatorException {
        if (logger.isDebugEnabled())
            logger.debug("Reading binlog index: " + indexFile.getAbsolutePath());

        // Open and read the index file.
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(this.indexFile);
            InputStreamReader reader = new InputStreamReader(fis);
            BufferedReader bufferedReader = new BufferedReader(reader);

            // Read and store each successive line.
            binlogFiles = new ArrayList<File>();
            String binlogName = null;
            while ((binlogName = bufferedReader.readLine()) != null) {
                File binlogFile = new File(this.binlogDirectory, binlogName.trim());
                binlogFiles.add(binlogFile);
            }
        } catch (FileNotFoundException e) {
            throw new MySQLExtractException("Binlog index file not found: " + indexFile.getAbsolutePath(), e);
        } catch (IOException e) {
            throw new MySQLExtractException("Error reading binlog index file: " + indexFile.getAbsolutePath(), e);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                }
            }
        }
    }

    public File nextBinlog(String binlogName) {
        if (this.binlogFiles == null)
            throw new IllegalStateException("Attempt to find next binlog before reading index");

        // Locate the file in the index after converting to a file to
        // ensure we compare base name to base name.
        int index = -1;
        File binlog = new File(this.binlogDirectory, binlogName);
        for (index = 0; index < binlogFiles.size(); index++) {
            if (binlog.getName().equals(binlogFiles.get(index).getName())) {
                break;
            }
        }
        if (index == -1) {
            // This might mean we have a corrupt index file or are confused.
            logger.warn("Index lookup on non-existent binlog file: " + binlogName);
            return null;
        }

        // Return the next file in the index if it exists.
        int nextIndex = index + 1;
        if ((nextIndex) < binlogFiles.size()) {
            return (binlogFiles.get(nextIndex));
        } else {
            return null;
        }
    }

    public List<File> getBinlogFiles() {
        return this.binlogFiles;
    }
}