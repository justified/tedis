/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.ReplicatorProperties;

public class PropertiesManager {
    private static Logger logger = Logger.getLogger(PropertiesManager.class);

    private ReplicatorProperties properties;

    private final File propertiesFile;

    public PropertiesManager(File propertiesFile) {
        this.propertiesFile = propertiesFile;
    }

    public synchronized ReplicatorProperties getProperties() {
        ReplicatorProperties rawProps = new ReplicatorProperties();
        rawProps.putAll(properties);

        Properties substitutedProps = rawProps.getProperties();
        ReplicatorProperties.substituteSystemValues(substitutedProps, 10);
        ReplicatorProperties props = new ReplicatorProperties();
        props.load(substitutedProps);
        return props;
    }

    public void loadProperties() throws ReplicatorException {
        logger.debug("Reading static properties file: " + propertiesFile.getAbsolutePath());
        properties = loadProperties(propertiesFile);
    }

    public static ReplicatorProperties loadProperties(File propsFile) throws ReplicatorException {
        try {
            ReplicatorProperties newProps = new ReplicatorProperties();
            newProps.load(new FileInputStream(propsFile), false);
            return newProps;
        } catch (FileNotFoundException e) {
            logger.error("Unable to find properties file: " + propsFile);
            throw new ReplicatorException("Unable to find properties file: " + e.getMessage());
        } catch (IOException e) {
            logger.error("Unable to read properties file: " + propsFile);
            throw new ReplicatorException("Unable to read properties file: " + e.getMessage());
        }
    }
}
