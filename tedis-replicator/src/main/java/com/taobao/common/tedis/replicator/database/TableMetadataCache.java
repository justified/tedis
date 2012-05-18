/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

public class TableMetadataCache implements CacheResourceManager<Table> {
    IndexedLRUCache<Table> cache;

    public TableMetadataCache(int capacity) {
        cache = new IndexedLRUCache<Table>(capacity, this);
    }

    public void release(Table metadata) {
        // Do nothing.
    }

    public int size() {
        return cache.size();
    }

    public void store(Table metadata) {
        String key = generateKey(metadata.getSchema(), metadata.getName());
        cache.put(key, metadata);
    }

    public Table retrieve(String schema, String tableName) {
        String key = generateKey(schema, tableName);
        return cache.get(key);
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }

    public int invalidateSchema(String schema) {
        return cache.invalidateByPrefix(schema);
    }

    public int invalidateTable(String schema, String tableName) {
        String key = generateKey(schema, tableName);
        return cache.invalidate(key);
    }

    public int invalidate(SqlOperation sqlOperation, String defaultSchema) {
        if (sqlOperation.getOperation() == SqlOperation.DROP && sqlOperation.getObjectType() == SqlOperation.SCHEMA) {
            return cache.invalidateByPrefix(sqlOperation.getSchema());
        } else if (sqlOperation.getOperation() == SqlOperation.DROP && sqlOperation.getObjectType() == SqlOperation.TABLE) {
            return invalidateTable(sqlOperation.getSchema(), defaultSchema, sqlOperation.getName());
        } else if (sqlOperation.getOperation() == SqlOperation.ALTER) {
            return invalidateTable(sqlOperation.getSchema(), defaultSchema, sqlOperation.getName());
        }
        return 0;
    }

    private String generateKey(String schema, String tableName) {
        StringBuffer key = new StringBuffer();
        key.append(schema);
        key.append(".");
        key.append(tableName);
        return key.toString();
    }

    private int invalidateTable(String schema, String defaultSchema, String tableName) {
        String key;
        if (schema == null)
            key = generateKey(defaultSchema, tableName);
        else
            key = generateKey(schema, tableName);
        return cache.invalidate(key);
    }
}