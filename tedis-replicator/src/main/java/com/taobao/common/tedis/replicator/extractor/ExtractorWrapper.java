/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.event.DBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.filter.Filter;
import com.taobao.common.tedis.replicator.plugin.PluginContext;

/**
 * This class wraps a basic Extractor so that it returns ReplDBMSEvent values
 * with assigned sequence numbers. It contains logic to recognize that we have
 * failed over; see {@link #setLastEvent(ReplDBMSHeader)} for more information.
 */
public class ExtractorWrapper implements Extractor {
    private static Logger logger = Logger.getLogger(ExtractorWrapper.class);
    private RawExtractor extractor;
    private String sourceId;
    private long seqno = 0;
    private short fragno = 0;
    private long epochNumber = 0;
    private List<Filter> autoFilters = new ArrayList<Filter>();

    public ExtractorWrapper(RawExtractor extractor) {
        this.extractor = extractor;
    }

    public RawExtractor getExtractor() {
        return extractor;
    }

    public ReplDBMSEvent extract() throws ReplicatorException, InterruptedException {
        DBMSEvent dbmsEvent = extractor.extract();

        // Generate the event.
        Timestamp extractTimestamp = dbmsEvent.getSourceTstamp();
        ReplDBMSEvent replEvent = new ReplDBMSEvent(seqno, fragno, dbmsEvent.isLastFrag(), sourceId, epochNumber, extractTimestamp, dbmsEvent);
        if (logger.isDebugEnabled())
            logger.debug("Source timestamp = " + dbmsEvent.getSourceTstamp() + " - Extracted timestamp = " + extractTimestamp);

        for (Filter filter : autoFilters) {
            try {
                replEvent = filter.filter(replEvent);
                if (replEvent == null)
                    return null;
            } catch (ReplicatorException e) {
                throw new ExtractorException("Auto-filter operation failed unexpectedly: " + e.getMessage(), e);
            }
        }

        // See if this is the last fragment.
        if (dbmsEvent.isLastFrag()) {
            seqno++;
            fragno = 0;
        } else
            fragno++;

        return replEvent;
    }

    public String getCurrentResourceEventId() throws ReplicatorException, InterruptedException {
        return extractor.getCurrentResourceEventId();
    }

    public boolean hasMoreEvents() {
        return false;
    }

    public void setLastEvent(ReplDBMSHeader header) throws ReplicatorException {
        String eventId;
        if (header == null) {
            seqno = 0;
            eventId = null;
        } else if (sourceId.equals(header.getSourceId())) {
            if (logger.isDebugEnabled())
                logger.debug("Source ID of max event verified: " + header.getSourceId());
            seqno = header.getSeqno() + 1;
            eventId = header.getEventId();
        } else {
            logger.info("Local source ID differs from last stored source ID: local=" + sourceId + " stored=" + header.getSourceId());
            logger.info("Restarting replication from scratch");

            seqno = header.getSeqno() + 1;
            eventId = null;
        }
        setLastEventId(eventId);
        epochNumber = seqno;
    }

    public void setLastEventId(String eventId) throws ReplicatorException {
        extractor.setLastEventId(eventId);
    }

    public void configure(PluginContext context) throws ReplicatorException, InterruptedException {
        logger.info("Configuring raw extractor and filter");
        sourceId = context.getSourceId();
        extractor.configure(context);
        for (Filter filter : autoFilters)
            filter.configure(context);
    }

    public void prepare(PluginContext context) throws ReplicatorException, InterruptedException {
        logger.info("Preparing raw extractor and filter");
        extractor.prepare(context);
        for (Filter filter : autoFilters)
            filter.prepare(context);
    }

    public void release(PluginContext context) throws ReplicatorException, InterruptedException {
        logger.info("Releasing raw extractor and filter");
        extractor.release(context);
        for (Filter filter : autoFilters)
            filter.release(context);
    }
}
