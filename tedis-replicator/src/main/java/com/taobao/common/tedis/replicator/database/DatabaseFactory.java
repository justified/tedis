/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

import java.sql.SQLException;

public class DatabaseFactory {
    public static Database createDatabase(String url, String user, String password) throws SQLException {
        return createDatabase(url, user, password, null);
    }

    public static Database createDatabase(String url, String user, String password, String vendor) throws SQLException {
        Database database;
        if (url.startsWith("jdbc:mysql"))
            database = new MySQLDatabase();
        else
            throw new RuntimeException("Unsupported URL type: " + url);

        database.setUrl(url);
        database.setUser(user);
        database.setPassword(password);

        return database;
    }

}
