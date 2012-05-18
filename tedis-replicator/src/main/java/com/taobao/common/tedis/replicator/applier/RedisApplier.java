/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.applier;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.commands.TedisManagerFactory;
import com.taobao.common.tedis.core.TedisManager;
import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.conf.DIYExecutor;
import com.taobao.common.tedis.replicator.conf.FailurePolicy;
import com.taobao.common.tedis.replicator.data.DBMSData;
import com.taobao.common.tedis.replicator.data.LoadDataFileFragment;
import com.taobao.common.tedis.replicator.data.OneRowChange;
import com.taobao.common.tedis.replicator.data.OneRowChange.ColumnSpec;
import com.taobao.common.tedis.replicator.data.OneRowChange.ColumnVal;
import com.taobao.common.tedis.replicator.data.RowChangeData;
import com.taobao.common.tedis.replicator.data.RowChangeData.ActionType;
import com.taobao.common.tedis.replicator.data.RowIdData;
import com.taobao.common.tedis.replicator.data.StatementData;
import com.taobao.common.tedis.replicator.event.DBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeaderData;
import com.taobao.common.tedis.replicator.plugin.PluginContext;
import com.taobao.common.tedis.replicator.redis.RedisHandleException;
import com.taobao.common.tedis.replicator.redis.RedisHandler;
import com.taobao.common.tedis.replicator.redis.RedisPreHandler;

public class RedisApplier implements RawApplier {

	private static Logger logger = Logger.getLogger(RedisApplier.class);

	// Task management information.
	private int taskId;

	private TedisManager tedisManager;
	private String appName = "test";
	private String version = "v0";

	// Latest event.
	private ReplDBMSHeader latestHeader;

	private Thread ioThread = Thread.currentThread();

	private List<RedisHandler> redisHandlers;
	private String redisClients;

	private int configNamespace = 0;

	private String configKey = "tedis_config_key";

	private FailurePolicy applierFailurePolicy;

	private String serviceName;

	@Override
	public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit, boolean doRollback) throws ReplicatorException, InterruptedException {
		ArrayList<DBMSData> dbmsDataValues = event.getData();

		for (DBMSData dbmsData : dbmsDataValues) {
			if (dbmsData instanceof StatementData) {
				logger.warn("Ignoring statement.");
			} else if (dbmsData instanceof RowChangeData) {
				RowChangeData rd = (RowChangeData) dbmsData;
				for (OneRowChange orc : rd.getRowChanges()) {
					ActionType action = orc.getAction();
					String schema = orc.getSchemaName();
					final String table = orc.getTableName();
					if (logger.isDebugEnabled()) {
						logger.debug("Processing row update: action=" + action + " schema=" + schema + " table=" + table);
					}
					if (action.equals(ActionType.INSERT)) {
						final Map<String, Object> maps = new HashMap<String, Object>();
						List<ColumnSpec> colSpecs = orc.getColumnSpec();

						Iterator<ArrayList<ColumnVal>> colValues = orc.getColumnValues().iterator();
						while (colValues.hasNext()) {
							ArrayList<ColumnVal> row = colValues.next();
							for (int i = 0; i < row.size(); i++) {
								String name = colSpecs.get(i).getName();
								Object value = row.get(i).getValue();
								maps.put(name, value);
							}
						}
						for (final RedisHandler redisHandler : redisHandlers) {
							try {
								if (!interested(redisHandler, table)) {
									continue;
								}
								final Executor executor = redisHandler.getExecutor();
								if (executor == null) {
									throw new ReplicatorException("Executor can not be null, implements RedisHandler's getExecutor method.");
								}
								Runnable task = new Runnable() {

									@Override
									public void run() {
										if (ioThread == Thread.currentThread() && executor != DIYExecutor.getInstance()) {
											logger.error("Cannot use the io thread to do biz task.");
											return;
										}

										if (redisHandler instanceof RedisPreHandler) {
											((RedisPreHandler) redisHandler).beforeInsert(tedisManager, table, maps);
										}

										redisHandler.insert(tedisManager, table, maps);
									}
								};

								executor.execute(task);
							} catch (Exception e) {
								if (applierFailurePolicy == FailurePolicy.STOP) {
									throw new ReplicatorException(e);
								} else {
									logger.warn("Redis handler error", e);
									continue;
								}
							}
						}
						if (logger.isDebugEnabled()) {
							logger.debug("Insert data " + maps);
						}
					} else if (action.equals(ActionType.UPDATE)) {
						final Map<String, Object> old = new HashMap<String, Object>();
						final Map<String, Object> maps = new HashMap<String, Object>();

						List<ColumnSpec> keySpecs = orc.getKeySpec();
						ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc.getKeyValues();

						List<ColumnSpec> colSpecs = orc.getColumnSpec();
						ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc.getColumnValues();

						for (int row = 0; row < columnValues.size() || row < keyValues.size(); row++) {
							List<ColumnVal> keyValuesOfRow = keyValues.get(row);
							List<ColumnVal> colValuesOfRow = columnValues.get(row);

							for (int i = 0; i < keyValuesOfRow.size(); i++) {
								String name = keySpecs.get(i).getName();
								Object value = keyValuesOfRow.get(i).getValue();
								old.put(name, value);
							}

							for (int i = 0; i < colValuesOfRow.size(); i++) {
								String name = colSpecs.get(i).getName();
								Object value = colValuesOfRow.get(i).getValue();
								maps.put(name, value);
							}
						}
						for (final RedisHandler redisHandler : redisHandlers) {
							try {
								if (!interested(redisHandler, table)) {
									continue;
								}
								final Executor executor = redisHandler.getExecutor();
								if (executor == null) {
									throw new ReplicatorException("Executor can not be null, implements RedisHandler's getExecutor method.");
								}
								Runnable task = new Runnable() {

									@Override
									public void run() {
										if (ioThread == Thread.currentThread() && executor != DIYExecutor.getInstance()) {
											logger.error("Cannot use the io thread to do biz task.");
											return;
										}

										if (redisHandler instanceof RedisPreHandler) {
											((RedisPreHandler) redisHandler).beforeUpdate(tedisManager, table, old, maps);
										}

										redisHandler.update(tedisManager, table, old, maps);
									}
								};

								executor.execute(task);
							} catch (Exception e) {
								if (applierFailurePolicy == FailurePolicy.STOP) {
									throw new ReplicatorException(e);
								} else {
									logger.warn("Redis handler error", e);
									continue;
								}
							}
						}
						if (logger.isDebugEnabled()) {
							logger.debug("Update " + old + " to " + maps);
						}
					} else if (action.equals(ActionType.DELETE)) {
						final Map<String, Object> maps = new HashMap<String, Object>();
						List<ColumnSpec> keySpecs = orc.getKeySpec();
						ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc.getKeyValues();
						ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc.getColumnValues();

						for (int row = 0; row < columnValues.size() || row < keyValues.size(); row++) {
							List<ColumnVal> keyValuesOfRow = keyValues.get(row);

							for (int i = 0; i < keyValuesOfRow.size(); i++) {
								String name = keySpecs.get(i).getName();
								Object value = keyValuesOfRow.get(i).getValue();
								maps.put(name, value);
							}
						}
						for (final RedisHandler redisHandler : redisHandlers) {
							try {
								if (!interested(redisHandler, table)) {
									continue;
								}
								final Executor executor = redisHandler.getExecutor();
								if (executor == null) {
									throw new ReplicatorException("Executor can not be null, implements RedisHandler's getExecutor method.");
								}
								Runnable task = new Runnable() {

									@Override
									public void run() {
										if (ioThread == Thread.currentThread() && executor != DIYExecutor.getInstance()) {
											logger.error("Cannot use the io thread to do biz task.");
											return;
										}

										if (redisHandler instanceof RedisPreHandler) {
											((RedisPreHandler) redisHandler).beforeDelete(tedisManager, table, maps);
										}

										redisHandler.delete(tedisManager, table, maps);
									}
								};

								executor.execute(task);
							} catch (Exception e) {
								if (applierFailurePolicy == FailurePolicy.STOP) {
									throw new ReplicatorException(e);
								} else {
									logger.warn("Redis handler error", e);
									continue;
								}
							}
						}
						if (logger.isDebugEnabled()) {
							logger.debug("Delete data " + maps);
						}
					} else {
						logger.warn("Unrecognized action type: " + action);
						return;
					}
				}
			} else if (dbmsData instanceof LoadDataFileFragment) {
				if (logger.isDebugEnabled())
					logger.debug("Ignoring load data file fragment");
			} else if (dbmsData instanceof RowIdData) {
				if (logger.isDebugEnabled())
					logger.debug("Ignoring row ID data");
			} else {
				logger.warn("Unsupported DbmsData class: " + dbmsData.getClass().getName());
			}
		}

		// Mark the current header and commit position if requested.
		this.latestHeader = header;
		if (doCommit) {
			commit();
		}
	}

	private boolean interested(RedisHandler handler, String table) throws RedisHandleException {
		String[] interests = handler.interest();
		if (interests != null) {
			for (String interest : interests) {
				if (interest.equals("*")) {
					return true;
				}
				if (table.startsWith(interest)) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public void commit() throws ReplicatorException, InterruptedException {
		if (latestHeader == null) {
			if (logger.isDebugEnabled())
				logger.debug("Unable to commit; last header is null");
			return;
		}
		try {
			Map<String, Object> prop = new HashMap<String, Object>();
			prop.put("task_id", taskId);
			prop.put("event_id", latestHeader.getEventId());
			prop.put("seqno", latestHeader.getSeqno());
			prop.put("last_frag", latestHeader.getLastFrag());
			prop.put("source_id", latestHeader.getSourceId());
			prop.put("fragno", latestHeader.getFragno());
			prop.put("extracted_timestamp", latestHeader.getExtractedTstamp());
			prop.put("epoch_number", latestHeader.getEpochNumber());
			String saveKey = configKey + "_" + serviceName;
			if (logger.isDebugEnabled()) {
				logger.debug("Saving properties:" + prop);
			}
			this.tedisManager.getHashCommands().putAll(configNamespace, saveKey, prop);
		} catch (Exception e) {
			throw new ReplicatorException(e);
		}
	}

	@Override
	public ReplDBMSHeader getLastEvent() throws ReplicatorException, InterruptedException {
		try {
			String savekey = configKey + "_" + serviceName;
			Map<String, Object> prop = this.tedisManager.getHashCommands().entries(configNamespace, savekey);
			if (logger.isDebugEnabled()) {
				logger.debug("Getting properties:" + prop);
			}
			if (prop == null) {
				return null;
			}
			String eventId = (String) prop.get("event_id");
			if (eventId == null) {
				return null;
			}
			long seqno = (Long) prop.get("seqno");
			boolean lastFrag = (Boolean) prop.get("last_frag");
			String sourceId = (String) prop.get("source_id");
			int fragno = (Integer) prop.get("fragno");
			Timestamp extractedTstamp = (Timestamp) prop.get("extracted_timestamp");
			long epochNumber = (Long) prop.get("epoch_number");
			ReplDBMSHeader last = new ReplDBMSHeaderData(seqno, (short) fragno, lastFrag, sourceId, epochNumber, eventId, extractedTstamp);
			return last;
		} catch (Exception e) {
			throw new ReplicatorException(e);
		}
	}

	@Override
	public void rollback() throws InterruptedException {
		// Nothing to do.
	}

	@Override
	public void setTaskId(int id) {
		this.taskId = id;
	}

	@Override
	public void configure(PluginContext context) throws ReplicatorException, InterruptedException {
		this.redisHandlers = new ArrayList<RedisHandler>();
		if (this.redisClients != null && !this.redisClients.isEmpty()) {
			String[] clients = redisClients.split(",");
			for (String client : clients) {
				RedisHandler handler = (RedisHandler) context.getExtension(client);
				if (handler == null) {
					logger.warn("Redis handler " + client + " not found, ignore it.");
					continue;
				}
				this.redisHandlers.add(handler);
			}
		}

		if (this.redisHandlers.size() == 0) {
			logger.warn("Find 0 handler, there should at least one redis handler.");
		}

		try {
			this.tedisManager = TedisManagerFactory.create(appName, version);
		} catch (Exception e) {
			throw new ReplicatorException("Redis init failed.", e);
		}

		this.serviceName = context.getServiceName();
		this.applierFailurePolicy = context.getApplierFailurePolicy();
	}

	@Override
	public void prepare(PluginContext context) throws ReplicatorException, InterruptedException {
		for (RedisHandler handler : redisHandlers) {
			handler.prepare(context);
		}
	}

	@Override
	public void release(PluginContext context) throws ReplicatorException, InterruptedException {
		if (redisHandlers != null) {
			for (RedisHandler handler : redisHandlers) {
				handler.release(context);
			}
		}
//		if (this.tedisManager != null) {
//			try {
//				TedisManagerFactory.destroy(this.tedisManager);
//			} catch (Exception e) {
//				// ignore
//			}
//		}
	}

	public void setRedisClients(String redisClients) {
		this.redisClients = redisClients;
	}

	public void setConfigNamespace(int configNamespace) {
		this.configNamespace = configNamespace;
	}

	public void setConfigKey(String configKey) {
		this.configKey = configKey;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

}
