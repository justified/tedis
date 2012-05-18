/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.mysql;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.sql.Connection;

import org.apache.log4j.Logger;

public class MySQLIOs {
    private final static String TSR_CONNECTION_CLASSNAME = "TSRConnection";
    private final static String C3P0_CONNECTION_CLASSNAME = "NewProxyConnection";
    private final static String MYSQL_CONNECTION_CLASSNAME_PREFIX = "com.mysql.jdbc";
    private final static String MYSQL_CONNECTION_CLASSNAME = MYSQL_CONNECTION_CLASSNAME_PREFIX + ".ConnectionImpl";
    private final static String DRIZZLE_CONNECTION_CLASSNAME = "org.drizzle.jdbc.DrizzleConnection";
    private static final Logger logger = Logger.getLogger(MySQLIOs.class);
    /** Where we will read data sent by the MySQL server */
    private final InputStream input;

    /** Where we will write data to the MySQL server */
    private final BufferedOutputStream output;

    /**
     * Constructor for internal use only - interface for retrieving IOs is
     * {@link #getMySQLIOs(Connection)}
     *
     * @param in
     *            where to read mysql server data from
     * @param out
     *            buffered stream for writing data to mysql server
     */
    private MySQLIOs(InputStream in, BufferedOutputStream out) {
        input = in;
        output = out;
    }

    /**
     * Extracts MySQL server input and output streams from the given connection. <br>
     * In order to avoid explicit driver dependency, this function uses
     * introspection to retrieve the connection inner field
     *
     * @param connection
     *            a jdbc connection object that must be connected to a MySQL
     *            server
     * @return a new MySQLIOs object containing extracted input and output
     *         streams of the given connection
     * @throws IOException
     *             if the streams could not be extracted
     */
    public static MySQLIOs getMySQLIOs(Connection connection) throws IOException {
        Object realConnection = connection;
        String className = realConnection.getClass().getSimpleName();

        // First, we need to get to the real, inner MySQL connection that is
        // possibly wrapped by the connection we received
        // Possible stacks:
        // 1/ router->MySQL
        // 2/ router->c3p0->MySQL
        // 3/ c3p0->MySQL
        // 4/ c3p0->router->MySQL

        if (TSR_CONNECTION_CLASSNAME.equals(className)) {
            realConnection = extractInnerConnectionFromSQLR(realConnection);
            if (realConnection != null)
                className = realConnection.getClass().getSimpleName();
        }
        if (C3P0_CONNECTION_CLASSNAME.equals(className)) {
            realConnection = extractInnerConnectionFromC3P0(realConnection);
            if (realConnection != null)
                className = realConnection.getClass().getSimpleName();
        }
        // loop one more time in case this is a c3p0->router stack
        if (TSR_CONNECTION_CLASSNAME.equals(className)) {
            realConnection = extractInnerConnectionFromSQLR(realConnection);
            if (realConnection != null)
                className = realConnection.getClass().getSimpleName();
        }

        if (realConnection == null) {
            throw new IOException("Could not get MySQL connection I/Os because inner connection is null");
        }

        // Here we know that realConnection is not null
        className = realConnection.getClass().getName();

        try {
            // MySQL connection IO extraction: need to get "io" field of
            // connection then inner in and output streams
            if (className.startsWith(MYSQL_CONNECTION_CLASSNAME_PREFIX)) {
                // with java 6, we'll get a JDBC4Connection which needs to be
                // down-casted to a ConnectionImpl
                Class<?> implClazz = Class.forName(MYSQL_CONNECTION_CLASSNAME);
                Field ioField = implClazz.getDeclaredField("io");
                ioField.setAccessible(true);
                Object ioObj = ioField.get(implClazz.cast(realConnection));
                return new MySQLIOs(getMysqlConnectionInputStream(ioObj), (BufferedOutputStream) getMysqlConnectionOutputStream(ioObj));
            }
            // Drizzle connection hold its i/Os in the "protocol" member
            // variable
            else if (className.equals(DRIZZLE_CONNECTION_CLASSNAME)) {
                Class<?> implClazz = realConnection.getClass();
                Field protocolField = implClazz.getDeclaredField("protocol");
                protocolField.setAccessible(true);
                Object protocolObj = protocolField.get(realConnection);
                return new MySQLIOs(getDrizzleConnectionInputStream(protocolObj), (BufferedOutputStream) getDrizzleConnectionOutputStream(protocolObj));
            } else {
                throw new IOException("Unknown connection type " + className + ". Cannot retrieve inner I/Os");
            }
        } catch (Exception e) {
            logger.error("Couldn't get connection IOs", e);
            throw new IOException(e.getLocalizedMessage());
        }
    }

    /**
     * Given a c3p0 connection, extracts the enclosed "real" connection
     *
     * @param c3p0Connection
     *            a c3p0-pooled connection
     * @return JDBC connection wrapped by the given connection
     * @throws IOException
     *             if an error occurs retrieving inner connection
     */
    private static Object extractInnerConnectionFromC3P0(Object c3p0Connection) throws IOException {
        if (logger.isTraceEnabled())
            logger.trace("Getting c3p0 inner connection");
        try {
            Field connectionField = c3p0Connection.getClass().getDeclaredField("inner");
            connectionField.setAccessible(true);
            c3p0Connection = (Connection) connectionField.get(c3p0Connection);
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage());
        }
        return c3p0Connection;
    }

    /**
     * Given a SQL-Router connection, retrieve the encapsulated connection
     *
     * @param sqlrConnection
     *             SQL-router connection
     * @return JDBC connection wrapped by the given router connection
     * @throws IOException
     *             if an error occurs retrieving inner connection
     */
    private static Object extractInnerConnectionFromSQLR(Object sqlrConnection) throws IOException {
        if (logger.isTraceEnabled())
            logger.trace("Getting SQL-Router inner connection");
        try {
            Field connectionField = sqlrConnection.getClass().getDeclaredField("realConnection");
            connectionField.setAccessible(true);
            sqlrConnection = connectionField.get(sqlrConnection);
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage());
        }
        return sqlrConnection;
    }

    /**
     * Uses java introspection to retrieve input stream from the given MysqlIO
     * object
     *
     * @param io
     *            the connection I/O field
     * @return the input stream of the connected mysql server
     * @throws IOException
     *             upon error while getting object
     */
    private static InputStream getMysqlConnectionInputStream(Object io) throws IOException {
        try {
            Field isField = io.getClass().getDeclaredField("mysqlInput");
            isField.setAccessible(true);
            return (InputStream) isField.get(io);
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage());
        }
    }

    /**
     * Uses java introspection to retrieve output stream from the given MysqlIO
     * object
     *
     * @param io
     *            the connection I/O field
     * @return the output stream of the connected mysql server
     * @throws IOException
     *             upon error while getting object
     */
    private static OutputStream getMysqlConnectionOutputStream(Object io) throws IOException {
        try {
            Field osField = io.getClass().getDeclaredField("mysqlOutput");
            osField.setAccessible(true);
            return (OutputStream) osField.get(io);
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage());
        }
    }

    private static InputStream getDrizzleConnectionInputStream(Object protocolObj) throws IOException {
        try {
            Field packetFetcherField = protocolObj.getClass().getDeclaredField("packetFetcher");
            packetFetcherField.setAccessible(true);
            Object packetFetcherObj = packetFetcherField.get(protocolObj);
            Field inputStreamField = packetFetcherObj.getClass().getDeclaredField("inputStream");
            inputStreamField.setAccessible(true);
            return (InputStream) inputStreamField.get(packetFetcherObj);
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage());
        }
    }

    private static OutputStream getDrizzleConnectionOutputStream(Object protocolObj) throws IOException {
        try {
            Field writerField = protocolObj.getClass().getDeclaredField("writer");
            writerField.setAccessible(true);
            return (OutputStream) writerField.get(protocolObj);
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage());
        }
    }

    /**
     * Tells whether the given connection is one that we can exploit for the
     * purposes of getting access to MySQL IOs.<br>
     *
     * @param conn
     *            connection to test
     * @return true if the connection is one of c3p0, SQL-Router, MySQL
     *         connector/j or Drizzle connector. Otherwise false.
     */
    public static boolean connectionIsCompatible(Connection conn) {
        if (conn == null)
            return false;
        String className = conn.getClass().getSimpleName();

        if (className.equals("NewProxyConnection") // c3p0
                || className.equals(DRIZZLE_CONNECTION_CLASSNAME) // Drizzle
                || className.equals("TSRConnection") // SQL router
                || className.startsWith(MYSQL_CONNECTION_CLASSNAME_PREFIX)) // MySQL
            return true;

        return false;
    }

    public InputStream getInput() {
        return input;
    }

    public BufferedOutputStream getOutput() {
        return output;
    }
}
