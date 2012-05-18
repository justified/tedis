/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import com.taobao.common.tedis.replicator.ReplicatorProperties;

public class ResultFormatter {
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final String keyHeader = "Key";
    private static final String valueHeader = "Value";
    private static final int keyMargin = 2;
    private static boolean printHeader = true;
    public static final String NEWLINE = "\n";
    private static int NOWRAP = -1;
    private static String indent = "";
    public static final String DEFAULT_INDENT = "  ";
    public static final String ROW_SEPARATOR = "-";
    public static final String ROW_BEGIN_END = "|";
    public static final int DEFAULT_WIDTH = 70;
    private static final int FIELD_SEPARATOR_WIDTH = 5;
    private static final String FIELD_SEPARATOR = "    ";

    protected final Object result;

    public ResultFormatter(Object result) {
        this.result = result;
        printHeader = true;
    }

    public ResultFormatter(Object result, boolean printHeader, String indentToUse) {
        this.result = result;
        printHeader = false;
        indent = indentToUse;
    }

    /**
     * Returns a formatted value.
     */
    public String format() {
        if (result == null) {
            return "(null)";
        } else if (result instanceof Map) {
            return format((Map<?, ?>) result);
        } else if (result instanceof ReplicatorProperties) {
            return format(((ReplicatorProperties) result).map());
        } else {
            return format(result);
        }
    }

    public String format(int wrapColumn) {
        if (result == null) {
            return "(null)";
        } else if (result instanceof Map) {
            return format((Map<?, ?>) result, wrapColumn);
        } else if (result instanceof ReplicatorProperties) {
            return format(((ReplicatorProperties) result).map(), wrapColumn);
        } else {
            return format(result);
        }

    }

    /**
     * Default object formatter using Object.toString().
     */
    protected String format(Object o) {
        return o.toString();
    }

    /**
     * Object formatter for Map instance. This sorts keys and then formats the
     * key value pairs into a nice tabular representation.
     */
    protected String format(Map<?, ?> props, int maxLength) {
        // Create writer instances to print formatted text.
        StringWriter writer = new StringWriter();
        PrintWriter printer = new PrintWriter(writer, true);

        // Organize the properties into a TreeMap for sorting and compute
        // dimensions along the way.
        Iterator<?> keyIterator = props.keySet().iterator();
        int keyLength = keyHeader.length();
        int valueLength = valueHeader.length();
        TreeMap<String, String[]> sortedProperties = new TreeMap<String, String[]>();
        while (keyIterator.hasNext()) {
            // Fetch values.
            String key = keyIterator.next().toString();
            Object rawValue = props.get(key);
            String value = (rawValue == null ? "null" : rawValue.toString());

            // Check character widths.
            if (key.length() > keyLength)
                keyLength = key.length();

            if (value.length() > valueLength)
                valueLength = value.length();

            String valueArray[] = wrap(value, maxLength, 0, false);

            // Store values again.
            sortedProperties.put(key, valueArray);
        }

        keyLength += keyMargin;

        if (maxLength != NOWRAP)
            valueLength = maxLength;

        // Compute a format string.
        String format = indent + "%-" + keyLength + "." + keyLength + "s %-" + valueLength + "." + valueLength + "s" + LINE_SEPARATOR;
        String formatHeader = indent + "%-" + keyLength + "." + keyLength + "s %-" + valueHeader.length() + "." + valueHeader.length() + "s" + LINE_SEPARATOR;

        // Write the output.

        if (printHeader) {
            printer.printf(formatHeader, keyHeader, valueHeader);
            printer.printf(formatHeader, "---", "-----");
        }

        for (String key : sortedProperties.keySet()) {
            String values[] = sortedProperties.get(key);
            printer.printf(format, key, values[0]);
            for (int i = 1; i < values.length; i++) {
                printer.printf(format, "", values[i]);
            }
        }

        printer.close();
        return writer.toString();
    }

    protected String format(Map<?, ?> props) {
        return format(props, NOWRAP);
    }

    /*
     * Utility method that wraps a string at an arbitrary length
     */
    public static String[] wrap(String value, int maxLength) {
        return wrap(value, maxLength, 0, false);
    }

    /*
     * Utility method that wraps a string at an arbitrary length
     */
    public static String[] wrap2(String value, int maxLength, boolean ignoreColon) {
        if (value.length() < maxLength || maxLength == NOWRAP)
            return new String[] { value };

        // Check to see if we are wrapping an pre-formatted
        // 'table' and accomodate that.
        int labelMarkerOffset = value.indexOf(":");
        int valueOffset = 0;

        if (labelMarkerOffset != -1 && !ignoreColon)
            valueOffset = labelMarkerOffset + FIELD_SEPARATOR_WIDTH;

        int currentLength = value.length();
        String nextSegment = value;

        Vector<String> results = new Vector<String>();

        int segmentCount = 0;

        while ((currentLength + valueOffset) > maxLength) {
            results.add(nextSegment.substring(0, maxLength));
            segmentCount++;

            if (nextSegment.length() > maxLength) {
                nextSegment = nextSegment.substring(maxLength);
                currentLength = nextSegment.length();
                if (segmentCount > 0 && nextSegment.length() > 0 && labelMarkerOffset != -1) {
                    nextSegment = padBefore(nextSegment, valueOffset, " ");
                }
            }
        }

        if (nextSegment.length() > 0) {
            results.add(nextSegment);
        }

        return results.toArray(new String[results.size()]);
    }

    public static String[] wrap(String value, int maxLength, boolean ignoreColon) {
        return wrap(value, maxLength, 0, ignoreColon);
    }

    public static String[] wrap(String value, int maxLength, int hangingIndent, boolean ignoreColon) {
        if (value.length() < maxLength || maxLength == NOWRAP)
            return new String[] { value };

        Vector<String> results = new Vector<String>();

        // Check to see if we are wrapping an pre-formatted
        // 'table' and accomodate that.
        int labelMarkerOffset = value.indexOf(":");
        int valueOffset = 0;
        String label = "";
        String nextSegment = "";
        if (labelMarkerOffset != -1 && !ignoreColon) {
            valueOffset = labelMarkerOffset + FIELD_SEPARATOR_WIDTH;
            label = value.substring(0, labelMarkerOffset + 1);
            value = value.substring(labelMarkerOffset + 2).trim();
            if (value.length() + valueOffset <= maxLength) {
                results.add(label + FIELD_SEPARATOR + value);
                return results.toArray(new String[results.size()]);
            }
            nextSegment = label + FIELD_SEPARATOR;
        }

        int indentToUse = 0;
        if (!ignoreColon && valueOffset > 0)
            indentToUse = valueOffset;
        else if (hangingIndent > 0)
            indentToUse = hangingIndent;

        String[] elements = value.split("\\s");
        // If elements, when split by spaces, are still too long, split them
        // up by length only.
        Vector<String> convertedElements = new Vector<String>();

        String elementToUse = "";
        for (String element : elements) {
            elementToUse = element;

            if (elementToUse.length() + valueOffset < maxLength) {
                convertedElements.add(elementToUse);
                elementToUse = "";
            } else {
                while (elementToUse.length() + valueOffset > maxLength) {
                    convertedElements.add(elementToUse.substring(0, maxLength - valueOffset - 1));
                    elementToUse = elementToUse.substring(maxLength - valueOffset - 1);
                }
            }
        }

        if (elementToUse.length() > 0)
            convertedElements.add(elementToUse);

        elements = convertedElements.toArray(new String[convertedElements.size()]);

        int segmentElementCount = 0;
        int segmentCount = 0;

        for (String element : elements) {
            elementToUse = element.trim();

            int totalSegmentLength = nextSegment.length() + elementToUse.length();
            if (segmentCount > 0)
                totalSegmentLength += valueOffset;

            if (totalSegmentLength > maxLength) {

                if (indentToUse > 0 && segmentCount > 0) {
                    nextSegment = padBefore(nextSegment, indentToUse, " ");
                }
                results.add(nextSegment);
                segmentCount++;
                nextSegment = "";
            }

            if (segmentElementCount++ > 0 && nextSegment.length() > 0)
                nextSegment = nextSegment + " ";

            nextSegment = nextSegment + elementToUse;

        }

        if (nextSegment.length() > 0) {
            if (indentToUse > 0 && results.size() > 1) {
                nextSegment = padBefore(nextSegment, indentToUse, " ");
            }
            results.add(nextSegment);
        }

        return results.toArray(new String[results.size()]);
    }

    public static String formatProperties(String entityName, ReplicatorProperties properties, boolean printHeader) {
        String[] columnNames = properties.map().keySet().toArray((new String[properties.map().size()]));
        Vector<String[]> results = new Vector<String[]>();
        results.add(properties.map().values().toArray(new String[properties.map().size()]));

        return formatResults(entityName, columnNames, results, 60, false, true);

    }

    public static String formatResults(String entityName, String[] columnNames, Vector<String[]> results, int entryWidth) {
        return formatResults(entityName, columnNames, results, entryWidth, false, true);
    }

    public static String formatResults(String entityName, String[] columnNames, Vector<String[]> results, int entryWidth, boolean ignoreColon) {
        return formatResults(entityName, columnNames, results, entryWidth, ignoreColon, true);
    }

    public static String formatResults(String entityName, String[] columnNames, Vector<String[]> results, int entryWidth, boolean ignoreColon, boolean useDelimiters) {
        StringBuilder builder = new StringBuilder();

        if (results == null) {
            return builder.toString();
        }

        int columnCount = columnNames != null ? columnNames.length : 1;
        String separator = makeSeparator(entryWidth, columnCount, useDelimiters);
        if (useDelimiters)
            builder.append(separator).append(NEWLINE);
        String row = makeRow(new String[] { entityName }, entryWidth, 0, ignoreColon, useDelimiters);
        builder.append(row);
        if (useDelimiters)
            builder.append(separator).append(NEWLINE);
        if (columnNames != null) {
            row = makeRow(columnNames, entryWidth, 0, ignoreColon, useDelimiters);
            builder.append(row).append(NEWLINE);
            if (useDelimiters)
                builder.append(separator).append(NEWLINE);
        }
        int rowCount = results.size();
        for (int i = 0; i < rowCount; i++) {
            row = makeRow(results.get(i), entryWidth, 0, ignoreColon, useDelimiters);
            builder.append(row);
        }
        if (useDelimiters)
            builder.append(separator).append(NEWLINE);

        return builder.toString();
    }

    public static String formatResult(String entityName, String result) {
        return formatResult(entityName, result, false, true);
    }

    public static String formatResult(String entityName, String result, boolean ignoreColon) {
        return formatResult(entityName, result, ignoreColon, true);
    }

    public static String formatResult(String entityName, String result, boolean ignoreColon, boolean useDelimiters) {
        StringBuilder builder = new StringBuilder();
        Vector<String[]> results = new Vector<String[]>();
        results.add(new String[] { result });

        builder.append(ResultFormatter.formatResults(entityName, null, results, DEFAULT_WIDTH, ignoreColon, useDelimiters)).append("\n");

        return builder.toString();

    }

    public static String makeRow(String[] entries, int entryWidth) {
        return makeRow(entries, entryWidth, 0, false, true);
    }

    public static String makeRow(String[] entries, int entryWidth, boolean ignoreColon) {
        return makeRow(entries, entryWidth, 0, ignoreColon, true);
    }

    public static String makeRow(String[] entries, int entryWidth, int indentToUse, boolean ignoreColon) {
        return makeRow(entries, entryWidth, indentToUse, ignoreColon, true);
    }

    public static String makeRow(String[] entries, int entryWidth, int indentToUse, boolean ignoreColon, boolean useDelimiters) {
        String row = "";
        String delimiter = "|";

        if (!useDelimiters)
            delimiter = " ";

        for (int i = 0; i < entries.length; i++) {
            String[] elements = entries[i].split("\\n");
            for (String element : elements) {
                String[] subElements = ResultFormatter.wrap(element.replace("\t", "  "), entryWidth - 2, indentToUse, ignoreColon);
                for (String subElement : subElements) {
                    row = row + delimiter;
                    row = row + padAfter(subElement, entryWidth, " ");
                    row = row + " " + delimiter + "\n";
                }
            }
        }
        return (row);
    }

    public static String makeDataRow(String[] entries, int entryWidth, int indentToUse, boolean ignoreColon, boolean useDelimiters) {
        String row = "";
        String delimiter = "|";

        if (!useDelimiters)
            delimiter = " ";

        for (int i = 0; i < entries.length; i++) {
            row = row + delimiter;
            row = row + padAfter(entries[i], entryWidth, " ");
            row = row + " ";// + delimiter + " ";
        }
        row = row + delimiter;

        return (row);
    }

    public static String makeCSVRow(String[] entries, int entryWidth) {
        String row = "";

        for (int i = 0; i < entries.length; i++) {
            if (row.length() > 0)
                row += ",";
            row += entries[i].trim();
        }
        return (row);
    }

    public static String makeSeparator(int entryWidth, int columnCount) {
        return makeSeparator(entryWidth, columnCount, true);
    }

    public static String makeSeparator(int entryWidth, int columnCount, boolean useDelimiters) {
        String entry = padAfter("", entryWidth + 1, "-");
        String delimiter = "+";
        String separator = delimiter;

        if (!useDelimiters) {
            separator = "";
            delimiter = "";
        }

        for (int i = 0; i < columnCount; i++) {
            separator = separator + entry + delimiter;
        }
        return (separator);
    }

    private static String padAfter(String orig, int size, String padChar) {
        if (orig == null) {
            orig = "<null>";
        }
        // Use StringBuffer, not just repeated String concatenation
        // to avoid creating too many temporary Strings.
        StringBuffer buffer = new StringBuffer("");
        buffer.append(orig);
        int extraChars = size - orig.length();
        for (int i = 0; i < extraChars; i++) {
            buffer.append(padChar);
        }

        return (buffer.toString());
    }

    private static String padBefore(String orig, int count, String padChar) {
        if (orig == null) {
            orig = "<null>";
        }
        // Use StringBuffer, not just repeated String concatenation
        // to avoid creating too many temporary Strings.
        StringBuffer buffer = new StringBuffer("");

        for (int i = 0; i < count; i++) {
            buffer.append(padChar);
        }
        buffer.append(orig);

        return (buffer.toString());
    }

    public static String formatMap(Map<String, ?> map) {
        StringBuilder builder = new StringBuilder();
        for (String key : map.keySet()) {
            Object value = map.get(key);
            builder.append(String.format("%30s:    %s\n", key, value));
        }

        return builder.toString();
    }
}
