/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator;

/**
 * Implements a time interval, which is expressed in milliseconds. Intervals can
 * be manipulated as long values but also translate to strings of the form
 * <p/>
 *
 * <pre>
 * NNN{d|h|m|s}
 * </pre>
 * <p/>
 * where NNN is a number and the letter following denotes a time unit of days,
 * hours, minutes, or seconds, respectively. If the time unit is left off the
 * value is assumed to be milliseconds.
 *
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class Interval {
    private long duration;

    public Interval(long duration) {
        this.duration = duration;
    }

    /**
     * Creates an interval from a string.
     */
    public Interval(String duration) throws NumberFormatException {
        this.duration = parseDurationString(duration);
    }

    // Parses the duration string.
    private long parseDurationString(String duration) throws NumberFormatException {
        // Parse out the number. Ensure we have a valid string with NNN + one
        // character for the unit type.
        duration = duration.trim();
        int index = 0;
        StringBuffer numberBuf = new StringBuffer();
        for (; index < duration.length() && Character.isDigit(duration.charAt(index)); index++) {
            numberBuf.append(duration.charAt(index));
        }
        if (index == 0 || index + 1 < duration.length())
            throw new NumberFormatException("Invalid interval format; must be NNN{d|h|m|s}: " + duration);

        // Convert the number. If we are at the end of the string, we are done.
        long number = new Long(numberBuf.toString());
        if (index == duration.length())
            return number;

        // Parse out the units.
        char unit = duration.charAt(index);
        int multiplier = -1;
        switch (Character.toLowerCase(unit)) {
        case 's':
            multiplier = 1000;
            break;
        case 'm':
            multiplier = 1000 * 60;
            break;
        case 'h':
            multiplier = 1000 * 60 * 60;
            break;
        case 'd':
            multiplier = 1000 * 60 * 60 * 24;
            break;
        default:
            throw new NumberFormatException("Invalid interval format; must be NNN{d|h|m|s}: " + duration);
        }

        return (long) number * multiplier;
    }

    /** Return interval as millisecond value. */
    public long longValue() {
        return duration;
    }

    /**
     * Returns true if the start and end times are greater than the duration of
     * this interval.
     *
     * @param startMillis
     *            Start time in milliseconds
     * @param endMillis
     *            End time in milliseconds
     */
    public boolean overInterval(long startMillis, long endMillis) {
        return (endMillis - startMillis) > duration;
    }

    /**
     * {@inheritDoc}
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object o) {
        if (o == null || !(o instanceof Interval))
            return false;
        else {
            return duration == ((Interval) o).longValue();
        }
    }
}
