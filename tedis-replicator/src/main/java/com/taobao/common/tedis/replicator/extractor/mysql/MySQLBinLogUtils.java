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
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.util.StatisticsMap;

public class MySQLBinLogUtils {
    private static final String WRITE_QUERY_PATTERN_STRING = "^(insert|update|delete|select\\s+into).*";
    private static final String CREATE_TEMPORARY_TABLE_STRING = "(^create\\s+temporary\\s+table\\s+)(\\w+\\b)";
    private static final String SYSTEM_DROP_TEMPORARY_TABLE_STRING = "(^drop.*!40005.*temporary.*\\s+table\\s+if\\s+exists)(.*)";
    private static final String SET_QUERY_PATTERN_STRING = "(^set\\s+.*)";
    private static final String USER_SET_QUERY_PATTERN_STRING = "(^set\\s+@`(\\w+)`)";
    private static final String BINARY_DATA_IN_WRITE = ".*\\s+_\\w+";

    private static final Pattern CREATE_TEMPORARY_TABLE_PATTERN = Pattern.compile(CREATE_TEMPORARY_TABLE_STRING, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern DROP_TEMPORARY_TABLE_PATTERN = Pattern.compile(SYSTEM_DROP_TEMPORARY_TABLE_STRING, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern WRITE_QUERY_PATTERN = Pattern.compile(WRITE_QUERY_PATTERN_STRING, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern SET_QUERY_PATTERN = Pattern.compile(SET_QUERY_PATTERN_STRING, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern USER_SET_QUERY_PATTERN = Pattern.compile(USER_SET_QUERY_PATTERN_STRING, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern BINARY_DATA_IN_WRITE_PATTERN = Pattern.compile(BINARY_DATA_IN_WRITE, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final String COMMAND_ENDING = "/*!*/;";
    private static final String INFO_LINE_START = "#";

    private static final String REFERENCED = "REFERENCED";
    private static final String WRITE_BINARY_DATA = "WRITE BINARY DATA";
    private static final String WRITE_WITH_EVANESCENT_VARIABLE = "WRITES WITH SESSION VARIABLE";
    private static final String WRITE_WITH_EVANESCENT_TABLE = "WRITES WITH TEMP TABLE";
    private static final String WRITE_WITH_EVANESCENT_UNKNOWN_TYPE = "WRITES WITH UNKNOWN TYPE";

    private Map<String, Vector<String>> sessionQueries = new LinkedHashMap<String, Vector<String>>();

    private static Logger logger = Logger.getLogger(MySQLBinLogUtils.class);

    static InputStreamReader converter;
    static BufferedReader in;

    private SessionManager sessionMgr = new SessionManager();

    private boolean addComments = true;
    private boolean printDetails = false;

    private final static String QUERIES = "QUERIES";
    private final static String EVANESCENT_QUERIES = "SESSION-SCOPE QUERIES";
    private final static String SESSION_COUNT = "SESSION COUNT";

    private int statusLength = 0;
    private int lineCount = 0;

    private enum SessionEntityType {
        CREATE_TEMPORARY_TABLE, DROP_TEMPORARY_TABLE, USER_SESSION_VARIABLE, SYSTEM_SESSION_VARIABLE, MISC_QUERY, WRITE
    }

    private class SessionManager extends HashMap<String, SessionContext> {
        private StatisticsMap statistics = new StatisticsMap("BINLOG SCAN STATISTICS");
        private static final long serialVersionUID = 1L;

        public SessionManager() {
            statistics.addLongStatistic(QUERIES);
            statistics.addLongStatistic(EVANESCENT_QUERIES);
            statistics.addLongStatistic(WRITE_WITH_EVANESCENT_VARIABLE);
            statistics.addLongStatistic(WRITE_WITH_EVANESCENT_TABLE);
            statistics.addLongStatistic(SESSION_COUNT);
            statistics.addLongStatistic(WRITE_BINARY_DATA);

            for (SessionEntityType e : EnumSet.range(SessionEntityType.CREATE_TEMPORARY_TABLE, SessionEntityType.WRITE)) {
                statistics.addLongStatistic(e.toString());
            }

        }

        public SessionContext getSession(String sessionId) {
            SessionContext session = get(sessionId);

            if (session == null) {
                session = new SessionContext(sessionId);
                put(sessionId, session);
                statistics.add(SESSION_COUNT, 1);
            }

            return session;
        }

        public void removeSession(String sessionId) {
            remove(sessionId);
        }

        public void addQuery(String sessionId, SessionQuery query) {
            getSession(sessionId).addQuery(query);
            statistics.add(QUERIES, 1);
        }

        public void addEvanescentQuery(String sessionId, SessionQuery query, String ref) {
            getSession(sessionId).addEvanescent(query, ref);
            statistics.add(EVANESCENT_QUERIES, 1);
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public void addEvanescentReference(String sessionId, String reference) {
            getSession(sessionId).addEvanescentReference(reference);
        }

        public StatisticsMap getStatistics() {
            return statistics;
        }
    }

    private class SessionContext {
        String sessionId = null;

        private StatisticsMap statistics = new StatisticsMap("SESSION STATISTICS");

        private final static String EVANESCENT_QUERY = "EVANESCENT QUERY";
        private final static String NORMAL_QUERY = "NORMAL QUERY";

        public SessionContext(String sessionId) {
            this.sessionId = sessionId;
            statistics.addLongStatistic(EVANESCENT_QUERY);
            statistics.addLongStatistic(NORMAL_QUERY);
        }

        private Vector<String> evanescentReferences = new Vector<String>();
        private Vector<SessionQuery> queries = new Vector<SessionQuery>();
        private Vector<SessionQuery> evanescentQueries = new Vector<SessionQuery>();

        public void addEvanescent(SessionQuery query, String ref) {
            evanescentQueries.add(query);
            evanescentReferences.add(ref);
            queries.add(query);
            statistics.add(EVANESCENT_QUERY, 1);
        }

        public void addQuery(SessionQuery query) {
            queries.add(query);
            statistics.add(NORMAL_QUERY, 1);
        }

        public void addEvanescentReference(String reference) {
            evanescentReferences.add(reference);
        }

        public String getSessionId() {
            return sessionId;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public void setSessionId(String sessionId) {
            this.sessionId = sessionId;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public Vector<String> getEvanescentReferences() {
            return evanescentReferences;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public void setEvanescentReferences(Vector<String> evanescentReferences) {
            this.evanescentReferences = evanescentReferences;
        }

        public Vector<SessionQuery> getQueries() {
            return queries;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public void setQueries(Vector<SessionQuery> queries) {
            this.queries = queries;
        }

        public Vector<SessionQuery> getEvanescentQueries() {
            return evanescentQueries;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public void setEvanescentQueries(Vector<SessionQuery> evanescentQueries) {
            this.evanescentQueries = evanescentQueries;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public StatisticsMap getStatistics() {
            return statistics;
        }
    }

    private class SessionQuery {
        private StatisticsMap statistics = new StatisticsMap("QUERY STATISTICS");

        private String sessionId;
        private int execTime;
        private int execError;
        private String query;
        private SessionEntityType type;
        private boolean isEvanescent = false;
        private Vector<String> foundReferences = new Vector<String>();

        public SessionQuery(String sessionId, String query, int execTime, int execError, SessionEntityType type) {
            this.sessionId = sessionId;
            this.query = query;
            this.execTime = execTime;
            this.execError = execError;
            this.type = type;
            statistics.addLongStatistic(REFERENCED);
        }

        public boolean references(String reference) {
            if (query.contains(reference)) {
                if (!foundReferences.contains(reference)) {
                    foundReferences.add(reference);
                    statistics.add(REFERENCED, 1);
                }

                return true;
            }

            return false;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public int referenceCount() {
            return foundReferences.size();
        }

        public void setIsEvanescent(boolean evanescent) {
            this.isEvanescent = evanescent;
        }

        public String toString() {
            return query;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public String getSessionId() {
            return sessionId;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public void setSessionId(String sessionId) {
            this.sessionId = sessionId;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public int getExecTime() {
            return execTime;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public void setExecTime(int execTime) {
            this.execTime = execTime;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public int getExecError() {
            return execError;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public void setExecError(int execError) {
            this.execError = execError;
        }

        public String getQuery() {
            return query;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public void setQuery(String query) {
            this.query = query;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public boolean isEvanescent() {
            return isEvanescent;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public void setEvanescent(boolean isEvanescent) {
            this.isEvanescent = isEvanescent;
        }

        public Vector<String> getFoundReferences() {
            return foundReferences;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public void setFoundReferences(Vector<String> foundReferences) {
            this.foundReferences = foundReferences;
        }

        // TODO Not used. To be removed ?
        @SuppressWarnings("unused")
        public StatisticsMap getStatistics() {
            return statistics;
        }

        public SessionEntityType getType() {
            return type;
        }

    }

    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                converter = new InputStreamReader(System.in);
                System.out.println("Getting input from stdin");
            } else {
                FileInputStream inFile = new FileInputStream(args[0]);
                converter = new InputStreamReader(inFile);
                System.out.println("Getting input from " + args[0]);
            }

            in = new BufferedReader(converter);

            MySQLBinLogUtils utils = new MySQLBinLogUtils();

            utils.parse(in);

            utils.analyze();

        } catch (Exception e) {
            System.out.println(e);
        }

    }

    private void analyze() {

        System.out.printf("\n\nPROCESSED %d LINES\n", lineCount);
        System.out.printf("\nRESULTS\n%s\n", sessionMgr.getStatistics());

        if (!printDetails)
            return;

        for (SessionContext session : sessionMgr.values()) {
            Vector<SessionQuery> queries = session.getQueries();
            System.out.printf("Session %s queries\n{\n", session.getSessionId());

            for (SessionQuery theQuery : queries) {
                System.out.printf("\t%s\n", theQuery);
            }
            System.out.printf("}\n", session.getSessionId());
        }
    }

    private void parse(BufferedReader in) {
        String line;
        String sessionId = null;
        Integer exec_time = 0;
        Integer error_code = 0;
        // Not used
        // long lastOffset = 0L;
        int queryCount = 0;
        boolean inQuery = false;
        StringBuilder query = new StringBuilder();
        String reference = null;

        while ((line = MySQLBinLogUtils.getLine()) != null) {
            if (lineCount++ % 1000 == 0) {
                printStatus(lineCount);
            }

            if (lineCount % 100000 == 0) {
                analyze();
            }

            if (inQuery) {
                if (line.startsWith(COMMAND_ENDING)) {
                    queryCount++;
                    logger.debug(String.format("Query, sessionId=%s, exec_time=%d, error_code=%d\n%s\n", sessionId, exec_time, error_code, query.toString()));

                    if ((reference = tempTableToCreate(query.toString())) != null) {
                        if (addComments) {
                            SessionQuery comment = new SessionQuery(sessionId, "/*!CREATE TEMPORARY TABLE FOLLOWS */", 0, 0, SessionEntityType.MISC_QUERY);
                            addSessionQuery(sessionId, comment);
                        }
                        SessionQuery qry = new SessionQuery(sessionId, query.toString(), exec_time, error_code, SessionEntityType.CREATE_TEMPORARY_TABLE);
                        addEvanescentQuery(sessionId, qry, reference);
                        sessionMgr.getStatistics().add(SessionEntityType.CREATE_TEMPORARY_TABLE.toString(), 1);

                    } else if ((reference = tempTableToDrop(query.toString())) != null) {
                        if (addComments) {
                            SessionQuery comment = new SessionQuery(sessionId, "/*!DROP TEMPORARY TABLE FOLLOWS */", 0, 0, SessionEntityType.MISC_QUERY);
                            addSessionQuery(sessionId, comment);

                        }
                        SessionQuery qry = new SessionQuery(sessionId, query.toString(), exec_time, error_code, SessionEntityType.DROP_TEMPORARY_TABLE);
                        addEvanescentQuery(sessionId, qry, reference);
                        sessionMgr.removeSession(sessionId);
                        sessionMgr.getStatistics().add(SessionEntityType.DROP_TEMPORARY_TABLE.toString(), 1);

                    } else if (WRITE_QUERY_PATTERN.matcher(query.toString()).matches()) {
                        SessionQuery qry = new SessionQuery(sessionId, query.toString(), exec_time, error_code, SessionEntityType.WRITE);

                        sessionMgr.getStatistics().add(SessionEntityType.WRITE.toString(), 1);

                        if (hasEvanescentReferences(qry, sessionMgr.getSession(sessionId), sessionMgr.getStatistics())) {
                            if (addComments) {
                                SessionQuery comment = new SessionQuery(sessionId, "/*!WRITE QUERY WITH EVANESCENT REFERENCES FOLLOWS */", 0, 0, SessionEntityType.MISC_QUERY);
                                addSessionQuery(sessionId, comment);

                            }

                        }
                        if (referencesBinaryData(qry.getQuery())) {
                            sessionMgr.getStatistics().add(WRITE_BINARY_DATA, 1);
                        }
                        addSessionQuery(sessionId, qry);
                        sessionMgr.getStatistics().add(SessionEntityType.WRITE.toString(), 1);

                    } else {
                        SessionQuery qry = new SessionQuery(sessionId, query.toString(), exec_time, error_code, SessionEntityType.MISC_QUERY);
                        addSessionQuery(sessionId, qry);
                        sessionMgr.getStatistics().add(SessionEntityType.MISC_QUERY.toString(), 1);
                    }

                    inQuery = false;

                } else if (SET_QUERY_PATTERN.matcher(line).matches()) {
                    exec_time = 0;
                    error_code = 0;
                    int commentIdx = line.indexOf(COMMAND_ENDING);
                    if (commentIdx != -1)
                        line = line.substring(0, commentIdx);

                    SessionQuery qry = new SessionQuery(sessionId, line, exec_time, error_code, SessionEntityType.SYSTEM_SESSION_VARIABLE);
                    addSessionQuery(sessionId, qry);
                    sessionMgr.getStatistics().add(SessionEntityType.SYSTEM_SESSION_VARIABLE.toString(), 1);
                } else if (line.endsWith(COMMAND_ENDING)) {
                    continue;
                } else if (line.startsWith("DELIMITER")) {
                    continue;
                } else {
                    if (query.length() > 0)
                        query.append(" ");

                    query.append(line);
                }
            } else if (line.startsWith(INFO_LINE_START)) {
                String elements[] = line.split(" +");
                if (elements.length == 8) {
                    String queryElements[] = elements[7].split("\t");
                    if (queryElements.length == 5) {
                        sessionId = queryElements[2].split("=")[1];
                        exec_time = Integer.valueOf(queryElements[3].split("=")[1]);
                        error_code = Integer.valueOf(queryElements[4].split("=")[1]);
                        query = new StringBuilder();
                        inQuery = true;
                    }
                    // Not used
                    // else if (elements.length == 3)
                    // {
                    // lastOffset = Long
                    // .valueOf(queryElements[2].split("=")[1]);
                    // }

                }
            } else if ((reference = userSetQuery(line)) != null) {

                int commentIdx = line.indexOf(COMMAND_ENDING);
                if (commentIdx != -1)
                    line = line.substring(0, commentIdx);

                if (addComments) {
                    SessionQuery comment = new SessionQuery(sessionId, "/*! EVANESCENT USER SET QUERY FOLLOWS */", 0, 0, SessionEntityType.MISC_QUERY);
                    addSessionQuery(sessionId, comment);
                }
                SessionQuery qry = new SessionQuery(sessionId, line, exec_time, error_code, SessionEntityType.USER_SESSION_VARIABLE);
                addEvanescentQuery(sessionId, qry, reference);
                sessionMgr.getStatistics().add(SessionEntityType.USER_SESSION_VARIABLE.toString(), 1);
            } else if (SET_QUERY_PATTERN.matcher(line).matches()) {
                exec_time = 0;
                error_code = 0;
                int commentIdx = line.indexOf(COMMAND_ENDING);
                if (commentIdx != -1)
                    line = line.substring(0, commentIdx);

                SessionQuery qry = new SessionQuery(sessionId, line, exec_time, error_code, SessionEntityType.SYSTEM_SESSION_VARIABLE);
                addSessionQuery(sessionId, qry);
                sessionMgr.getStatistics().add(SessionEntityType.SYSTEM_SESSION_VARIABLE.toString(), 1);
            }

        }
    }

    private void addSessionQuery(String sessionId, SessionQuery query) {
        sessionMgr.addQuery(sessionId, query);
    }

    private void addEvanescentQuery(String sessionId, SessionQuery query, String ref) {
        query.setIsEvanescent(true);
        query.references(ref);
        sessionMgr.addEvanescentQuery(sessionId, query, ref);
    }

    // Read a String from standard system input
    public static String getLine() {
        try {
            return in.readLine();
        } catch (Exception e) {
            return null;
        }
    }

    public Map<String, Vector<String>> getSessionQueries() {
        return sessionQueries;
    }

    public void setSessionQueries(Map<String, Vector<String>> sessionQueries) {
        this.sessionQueries = sessionQueries;
    }

    public static String tempTableToCreate(String query) {
        boolean foundTempTable = false;
        Matcher matcher = CREATE_TEMPORARY_TABLE_PATTERN.matcher(query);

        foundTempTable = matcher.find();

        if (foundTempTable) {
            // Return the table name
            return matcher.group(2);

        }
        return null;
    }

    public static String tempTableToDrop(String query) {
        boolean foundTempTable = false;
        Matcher matcher = DROP_TEMPORARY_TABLE_PATTERN.matcher(query);

        foundTempTable = matcher.find();

        if (foundTempTable) {
            // Return the table name
            return matcher.group(2);

        }
        return null;
    }

    public static String userSetQuery(String query) {
        boolean foundSetQuery = false;
        Matcher matcher = USER_SET_QUERY_PATTERN.matcher(query);

        foundSetQuery = matcher.find();

        if (foundSetQuery) {
            // Return the table name
            return matcher.group(2);

        }
        return null;
    }

    public static boolean referencesBinaryData(String query) {
        Matcher matcher = BINARY_DATA_IN_WRITE_PATTERN.matcher(query);

        return matcher.find();
    }

    public static boolean hasEvanescentReferences(SessionQuery query, SessionContext session, StatisticsMap statistics) {
        int referenceCount = 0;

        for (SessionQuery eQuery : session.getEvanescentQueries()) {
            for (String reference : eQuery.getFoundReferences()) {
                if (query.references(reference)) {
                    referenceCount++;
                    if (eQuery.getType() == SessionEntityType.CREATE_TEMPORARY_TABLE)
                        statistics.add(WRITE_WITH_EVANESCENT_TABLE, 1);
                    else if (eQuery.getType() == SessionEntityType.USER_SESSION_VARIABLE)
                        statistics.add(WRITE_WITH_EVANESCENT_VARIABLE, 1);
                    else
                        statistics.add(WRITE_WITH_EVANESCENT_UNKNOWN_TYPE, 1);
                }
            }
        }

        if (referenceCount > 0)
            return true;

        return false;

    }

    private void printStatus(int lineCount) {
        if (statusLength++ % 6 == 5) {
            System.out.print(" ==>\n");
        }

        System.out.printf("%d ", lineCount - 1);

    }
}