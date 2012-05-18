/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

public class MySQLOperationStringBuilder {
    // Parameters.
    private int maxOutputLength;

    // Input string and state thereof.
    private String inputString;
    private int inputLength;
    private int inputIndex;

    // Output string buffer.
    private StringBuffer outputString = new StringBuffer();

    /** Create instance. */
    public MySQLOperationStringBuilder(int maxOutputLength) {
        this.maxOutputLength = maxOutputLength;
    }

    /** Build a string for parsing. */
    public String build(String input) {
        // Set up parameter for build operation.
        inputString = input;
        inputIndex = 0;
        inputLength = input.length();
        outputString = new StringBuffer();

        // Loop until we run out of input or output.
        char nextChar;
        while ((nextChar = get()) != 0 && outputString.length() < maxOutputLength) {
            if (nextChar == '/') {
                if (inputStartsWith("*!")) {
                    // Look ahead to ensure we have a bang comment.
                    String nextChars = peek(7);
                    boolean haveBangComment = false;
                    // Peek returns null if there is not enough data to be read.
                    if (nextChars != null && nextChars.length() == 7) {
                        haveBangComment = true;
                        for (int i = 2; i < nextChars.length(); i++) {
                            if (!Character.isDigit(nextChars.charAt(i))) {
                                haveBangComment = false;
                                break;
                            }
                        }
                    }

                    if (haveBangComment) {
                        // Strip the enclosing comment characters and add
                        // content.
                        skip(7);
                        String contents = getToDelimiter("*/");
                        if (contents != null) {
                            put(contents);
                            skip(2);
                        }
                    } else {
                        // No comment after all, so just add it.
                        put(nextChar);
                    }
                } else if (inputStartsWith("*")) {
                    // Skip the entire comment.
                    if (getToDelimiter("*/") != null)
                        skip(2);
                } else {
                    // Just add it.
                    put(nextChar);
                }
            } else if (nextChar == '-') {
                if (inputStartsWith("-")) {
                    // Ensure there is whitespace or end-of-input after the
                    // comment. MySQL requires whitespace.
                    String tail = peek(2);
                    if (tail == null) {
                        // Trailing comment, so we are done. It will be dropped.
                        break;
                    } else {
                        // Look for whitespace following "--" comment.
                        if (Character.isWhitespace(tail.charAt(1))) {
                            // Skip the comment and put in a space instead.
                            String buf = getToEndOfLine();
                            skip(buf.length());
                            put(' ');
                        } else
                            put(nextChar);
                    }
                } else {
                    // Just add it.
                    put(nextChar);
                }
            } else if (nextChar == '\n') {
                // Convert to space.
                put(" ");
            } else {
                put(nextChar);
            }
        }

        // Return what we found.
        return outputString.toString();
    }

    // Returns the next character in the input string provided we
    // have one.
    private char get() {
        if (inputIndex < inputLength)
            return inputString.charAt(inputIndex++);
        else
            return 0;
    }

    // Preview the next N characters or return null if there are not enough of
    // them.
    private String peek(int n) {
        int endIndex = inputIndex + n;
        if (endIndex <= inputLength)
            return inputString.substring(inputIndex, endIndex);
        else
            return null;
    }

    // Return string characters up to but not including delimiter.
    private String getToDelimiter(String delimiter) {
        String content = null;
        if (inputIndex < inputLength) {
            int delimiterIndex = inputString.indexOf(delimiter, inputIndex);
            if (delimiterIndex > -1) {
                content = inputString.substring(inputIndex, delimiterIndex);
                inputIndex = delimiterIndex;
            }
        }

        return content;
    }

    // Skip characters up to but not including end-of-line.
    private String getToEndOfLine() {
        String lineSeparator = System.getProperty("line.separator");
        int endIndex = inputIndex;
        while (endIndex < inputLength) {
            char c = inputString.charAt(endIndex++);
            if (lineSeparator.indexOf(c) > -1)
                break;
        }
        return inputString.substring(inputIndex, endIndex);
    }

    // Returns true if the input starts with the argument at the
    // current index.
    private boolean inputStartsWith(String prefix) {
        if (inputIndex < inputLength)
            return inputString.startsWith(prefix, inputIndex);
        else
            return false;
    }

    // Skip over the desired number of characters.
    private void skip(int n) {
        if (inputIndex < inputLength)
            inputIndex += n;
    }

    // Adds a character to the output.
    private void put(char c) {
        if (!Character.isWhitespace(c) || outputString.length() != 0) {
            outputString.append(c);
        }
    }

    // Adds a string to the output.
    private void put(String s) {
        for (int i = 0; i < s.length(); i++)
            put(s.charAt(i));
    }
}