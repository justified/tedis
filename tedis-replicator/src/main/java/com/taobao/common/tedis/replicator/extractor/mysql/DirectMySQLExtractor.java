/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.conf.FailurePolicy;
import com.taobao.common.tedis.replicator.conf.ReplicatorRuntime;
import com.taobao.common.tedis.replicator.data.DBMSData;
import com.taobao.common.tedis.replicator.data.LoadDataFileDelete;
import com.taobao.common.tedis.replicator.data.LoadDataFileFragment;
import com.taobao.common.tedis.replicator.data.LoadDataFileQuery;
import com.taobao.common.tedis.replicator.data.RowChangeData;
import com.taobao.common.tedis.replicator.data.RowIdData;
import com.taobao.common.tedis.replicator.data.StatementData;
import com.taobao.common.tedis.replicator.database.MySQLOperationMatcher;
import com.taobao.common.tedis.replicator.database.SqlOperation;
import com.taobao.common.tedis.replicator.database.SqlOperationMatcher;
import com.taobao.common.tedis.replicator.event.DBMSEmptyEvent;
import com.taobao.common.tedis.replicator.event.DBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplOption;
import com.taobao.common.tedis.replicator.event.ReplOptionParams;
import com.taobao.common.tedis.replicator.extractor.ExtractorException;
import com.taobao.common.tedis.replicator.extractor.RawExtractor;
import com.taobao.common.tedis.replicator.plugin.PluginContext;

public class DirectMySQLExtractor implements RawExtractor {
	private static Logger logger = Logger.getLogger(DirectMySQLExtractor.class);

	private ReplicatorRuntime runtime = null;
	private String host = "localhost";
	private int port = 3306;
	private String user = "root";
	private String password = "";
	private boolean strictVersionChecking = true;

	private LogExtractor logExtractor = null;

	private static long binlogPositionMaxLength = 10;
	private String binlogFilePattern = "mysql-bin";
	private int queueCapacity = 8192;
	private boolean useThreadPool = false;
	private int corePoolSize = 8;
	private int maximumPoolSize = 8;

	// SQL parser.
	SqlOperationMatcher sqlMatcher = new MySQLOperationMatcher();

	private HashMap<Long, TableMapLogEvent> tableEvents = new HashMap<Long, TableMapLogEvent>();

	private int transactionFragSize = 1024;
	private boolean fragmentedTransaction = false;

	// Varchar type fields can be retrieved and stored in THL either using
	// String datatype or bytes arrays. By default, using string datatype.
	private boolean useBytesForStrings = false;

	// Should schema name be prefetched when a Load Data Infile Begin event is
	// extracted ?
	private boolean prefetchSchemaNameLDI = true;

	private HashMap<Integer, String> loadDataSchemas;

	private String jdbcHeader;

	private int serverId = 125;
	private FailurePolicy extractorFailurePolicy;

	public FailurePolicy getExtractorFailurePolicy() {
		return extractorFailurePolicy;
	}

	public void setExtractorFailurePolicy(FailurePolicy extractorFailurePolicy) {
		this.extractorFailurePolicy = extractorFailurePolicy;
	}

	public int getServerId() {
		return serverId;
	}

	public void setServerId(int serverId) {
		this.serverId = serverId;
	}

	public String getBinlogFilePattern() {
		return binlogFilePattern;
	}

	public void setBinlogFilePattern(String binlogFilePattern) {
		this.binlogFilePattern = binlogFilePattern;
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

	public boolean isUseThreadPool() {
		return useThreadPool;
	}

	public void setUseThreadPool(boolean useThreadPool) {
		this.useThreadPool = useThreadPool;
	}

	public int getCorePoolSize() {
		return corePoolSize;
	}

	public void setCorePoolSize(int corePoolSize) {
		this.corePoolSize = corePoolSize;
	}

	public int getMaximumPoolSize() {
		return maximumPoolSize;
	}

	public void setMaximumPoolSize(int maximumPoolSize) {
		this.maximumPoolSize = maximumPoolSize;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public boolean isStrictVersionChecking() {
		return strictVersionChecking;
	}

	public void setStrictVersionChecking(boolean strictVersionChecking) {
		this.strictVersionChecking = strictVersionChecking;
	}

	public boolean isUsingBytesForString() {
		return useBytesForStrings;
	}

	public void setUsingBytesForString(boolean useBytes) {
		this.useBytesForStrings = useBytes;
	}

	public int getTransactionFragSize() {
		return transactionFragSize;
	}

	public void setTransactionFragSize(int transactionFragSize) {
		this.transactionFragSize = transactionFragSize;
	}

	public String getJdbcHeader() {
		return jdbcHeader;
	}

	public void setJdbcHeader(String jdbcHeader) {
		this.jdbcHeader = jdbcHeader;
	}

	private static String getDBMSEventId(BinlogPosition binlogPosition, long sessionId) {
		String fileName = binlogPosition.getFileName();
		String binlogNumber = fileName.substring(fileName.lastIndexOf('.') + 1);
		String position = getPositionAsString(binlogPosition.getPosition());
		return String.format("%s:%s;%d", binlogNumber, position, sessionId);
	}

	private static String getPositionAsString(long position) {
		return String.format("%0" + (binlogPositionMaxLength + 1) + "d", new Long(position));
	}

	private DBMSEvent extractEvent() throws ReplicatorException, InterruptedException {
		BinlogPosition position = null;
		boolean inTransaction = fragmentedTransaction;
		fragmentedTransaction = false;
		boolean autocommitMode = true;
		boolean doFileFragment = false;
		Timestamp startTime = null;

		long sessionId = 0;
		ArrayList<DBMSData> dataArray = new ArrayList<DBMSData>();

		boolean foundRowsLogEvent = false;
		LinkedList<ReplOption> savedOptions = new LinkedList<ReplOption>();

		try {
			String defaultDb = null;
			RowChangeData rowChangeData = null;
			long fragSize = 0;
			int serverId = -1;
			while (true) {
				DBMSEvent dbmsEvent = null;
				EventAndPosition eventAndPosition = logExtractor.extractLogEvent();
				if (eventAndPosition == null) {
					if (position != null) {
						return new DBMSEmptyEvent(getDBMSEventId(position, sessionId));
					}
					continue;
				}
				position = eventAndPosition.getPosition();

				LogEvent logEvent = eventAndPosition.getEvent();
				if (logEvent == null) {
					logger.debug("Unknown binlog field, skipping");
					continue;
				}

				if (serverId == -1)
					serverId = logEvent.serverId;

				if (startTime == null)
					startTime = logEvent.getWhen();

				if (logEvent instanceof RowsLogEvent) {
					fragSize += ((RowsLogEvent) logEvent).getEventSize();
				}
				if (logger.isDebugEnabled())
					logger.debug("Current fragment size=" + fragSize);

				/*
				 * TRANSACTION AGGREGATION PROBLEM We need this variable to
				 * determine when we commit SQL event - event must contain whole
				 * number of transactions (one of the requirements for parallel
				 * application on slaves), preferably 1. Now MySQL binlog
				 * behaves differently for different table types when in
				 * AUTOCOMMIT mode. Updates to InnoDB tables are always followed
				 * by explicit COMMIT event, updates to MyISAM - not. So we
				 * cannot rely solely on COMMIT events to denote the end of
				 * transaction and have to develop a complex heuristics to
				 * determine whether we wait for COMMIT event or replicate
				 * immediately.
				 */
				boolean doCommit = false;
				boolean doRollback = false;

				boolean unsafeForBlockCommit = false;

				if (logEvent.getClass() == QueryLogEvent.class) {
					QueryLogEvent event = (QueryLogEvent) logEvent;
					String queryString = event.getQuery();
					StatementData statement = new StatementData(queryString);

					// Extract the charset name if it can be found.
					String charsetName = event.getCharsetName();
					if (charsetName != null)
						statement.addOption(ReplOptionParams.JAVA_CHARSET_NAME, event.getCharsetName());
					if (logger.isDebugEnabled())
						logger.debug("Query extracted: " + queryString + " charset=" + charsetName);

					// Parse for SQL metadata and add to the statement.
					String query;

					if (!useBytesForStrings)
						query = queryString;
					else {
						// Translate only a few bytes, in order to eventually
						// detect some keywords.
						int len = Math.min(event.getQueryAsBytes().length, 200);
						if (charsetName == null)
							query = new String(event.getQueryAsBytes(), 0, len);
						else
							query = new String(event.getQueryAsBytes(), 0, len, charsetName);
					}
					SqlOperation sqlOperation = sqlMatcher.match(query);
					statement.setParsingMetadata(sqlOperation);

					// We must commit on DDLs and the like except for BEGIN or
					// START TRANSACTION, since they start new transaction at
					// the same time
					doCommit = !inTransaction || sqlOperation.isAutoCommit();
					int operation = sqlOperation.getOperation();
					if (operation == SqlOperation.BEGIN) {
						inTransaction = true;
						doCommit = false;
						// This a a BEGIN statement : buffer session variables
						// for following row events if any and skip it

						/* Adding statement options */
						savedOptions.add(new ReplOption("autocommit", event.getAutocommitFlag()));
						savedOptions.add(new ReplOption("sql_auto_is_null", event.getAutoIsNullFlag()));
						savedOptions.add(new ReplOption("foreign_key_checks", event.getForeignKeyChecksFlag()));
						savedOptions.add(new ReplOption("unique_checks", event.getUniqueChecksFlag()));
						savedOptions.add(new ReplOption("sql_mode", event.getSqlMode()));
						savedOptions.add(new ReplOption("character_set_client", String.valueOf(event.getClientCharsetId())));
						savedOptions.add(new ReplOption("collation_connection", String.valueOf(event.getClientCollationId())));
						savedOptions.add(new ReplOption("collation_server", String.valueOf(event.getServerCollationId())));

						if (event.getAutoIncrementIncrement() >= 0)
							savedOptions.add(new ReplOption("auto_increment_increment", String.valueOf(event.getAutoIncrementIncrement())));

						if (event.getAutoIncrementOffset() >= 0)
							savedOptions.add(new ReplOption("auto_increment_offset", String.valueOf(event.getAutoIncrementOffset())));

						continue;
					}

					if (operation == SqlOperation.COMMIT) {
						// This is a COMMIT statement : dropping it for now
						// Temporary workaround for TREP-243
						doCommit = true;
						inTransaction = !autocommitMode;
					} else if (operation == SqlOperation.ROLLBACK) {
						doRollback = true;
						inTransaction = !autocommitMode;

					} else {
						// some optimization: it makes sense to check for
						// 'CREATE DATABASE' only if we know that it is not
						// regular DML - this is a fix for TREP-52 - attempt
						// to use DB which hasn't been created yet.
						boolean isCreateOrDropDB = sqlOperation.getObjectType() == SqlOperation.SCHEMA;
						boolean prependUseDb = !(sqlOperation.isAutoCommit() && isCreateOrDropDB);

						if (defaultDb == null) {
							// first query in transaction
							sessionId = event.getSessionId();
							if (prependUseDb) {
								defaultDb = event.getDefaultDb();
								statement.setDefaultSchema(defaultDb);
							}
						} else {
							// check that session ID is the same
							assert (sessionId == event.getSessionId());
							// check if default database has changed
							String newDb = event.getDefaultDb();
							if (!defaultDb.equals(newDb) && prependUseDb) {
								defaultDb = newDb;
								statement.setDefaultSchema(newDb);
							}
						}
						if (isCreateOrDropDB)
							statement.addOption(StatementData.CREATE_OR_DROP_DB, "");

						if (operation == SqlOperation.CREATE || operation == SqlOperation.DROP || operation == SqlOperation.ALTER || operation == SqlOperation.UNRECOGNIZED)
							unsafeForBlockCommit = true;

						statement.setTimestamp(event.getWhen().getTime());
						if (!useBytesForStrings) {
							statement.setQuery(queryString);
							fragSize += queryString.length();
						} else {
							byte[] bytes = event.getQueryAsBytes();
							statement.setQuery(bytes);
							fragSize += bytes.length;
						}

						/* Adding statement options */
						statement.addOption("autocommit", event.getAutocommitFlag());
						statement.addOption("sql_auto_is_null", event.getAutoIsNullFlag());
						statement.addOption("foreign_key_checks", event.getForeignKeyChecksFlag());
						statement.addOption("unique_checks", event.getUniqueChecksFlag());

						if (event.getAutoIncrementIncrement() >= 0)
							statement.addOption("auto_increment_increment", String.valueOf(event.getAutoIncrementIncrement()));

						if (event.getAutoIncrementOffset() >= 0)
							statement.addOption("auto_increment_offset", String.valueOf(event.getAutoIncrementOffset()));

						/* Adding statement sql_mode */
						statement.addOption("sql_mode", event.getSqlMode());

						/* Adding character set / collation information */
						statement.addOption("character_set_client", String.valueOf(event.getClientCharsetId()));
						statement.addOption("collation_connection", String.valueOf(event.getClientCollationId()));
						statement.addOption("collation_server", String.valueOf(event.getServerCollationId()));
						statement.setErrorCode(event.getErrorCode());

						dataArray.add(statement);
					}
				} else if (logEvent.getClass() == UserVarLogEvent.class) {
					logger.debug("USER_VAR_EVENT detected: " + ((UserVarLogEvent) logEvent).getQuery());
					dataArray.add(new StatementData(((UserVarLogEvent) logEvent).getQuery()));

				} else if (logEvent.getClass() == RandLogEvent.class) {
					logger.debug("RAND_EVENT detected: " + ((RandLogEvent) logEvent).getQuery());
					dataArray.add(new StatementData(((RandLogEvent) logEvent).getQuery()));
				} else if (logEvent.getClass() == IntvarLogEvent.class) {
					IntvarLogEvent intvarLogEvent = (IntvarLogEvent) logEvent;
					if (logger.isDebugEnabled())
						logger.debug("INTVAR_EVENT detected, value: " + intvarLogEvent.getValue() + " / type : " + intvarLogEvent.getType());
					/*
					 * For MySQL applying, we could have following SET
					 * statement: dataArray.add(new StatementData( "SET
					 * INSERT_ID= " + ((Intvar_log_event)
					 * logEvent).getValue()));
					 */
					dataArray.add(new RowIdData(intvarLogEvent.getValue(), intvarLogEvent.getType()));
				} else if (logEvent.getClass() == XidLogEvent.class) {
					logger.debug("Commit extracted: " + ((XidLogEvent) logEvent).getXid());
					// If there's nothing to commit, just ignore.
					// Should happen for InnoDB tables in AUTOCOMMIT mode.
					if (!dataArray.isEmpty()) {
						doCommit = true;
					}
					if (rowChangeData != null) {
						doCommit = true;
					}
					// It seems like there's always explicit COMMIT event if
					// transaction is implicitely committed,
					// but does transaction start implicitely?
					inTransaction = !autocommitMode;

					if (!doCommit) {
						logger.debug("Clearing Table Map events");
						tableEvents.clear();
						tableEvents = new HashMap<Long, TableMapLogEvent>();
						return new DBMSEmptyEvent(getDBMSEventId(position, sessionId));
					}
				} else if (logEvent.getClass() == StopLogEvent.class) {
					logger.debug("Stop event extracted: ");
					String stopEventId = getDBMSEventId(position, sessionId);
					logger.info("Server stop event in log: " + stopEventId);
					/*
					 * It is possible that mysqld is not running anymore, try if
					 * new binlog position can be found. Otherwise we need to
					 * bail out to prevent log inconsistencies.
					 */
					logger.warn("MySQL appears to have stopped the binary log; retrying");

					try {
						logExtractor.nextBinlogFile(position);
					} catch (ExtractorException e) {
						/*
						 * mysql has shutdown for good, need to replicate
						 * pending transaction and then retry connecting
						 */
					}
				} else if (logEvent.getClass() == RotateLogEvent.class) {
					String newBinlogFilename = ((RotateLogEvent) logEvent).getNewBinlogFilename();
					logger.debug("Rotate log event: new binlog=" + newBinlogFilename);
					logExtractor.setBinlogFile(newBinlogFilename);
				} else if (logEvent.getClass() == TableMapLogEvent.class) {
					logger.debug("got table map event");
					// remember last table map event
					TableMapLogEvent tableEvent = (TableMapLogEvent) logEvent;
					tableEvents.put(tableEvent.getTableId(), tableEvent);
				} else if (logEvent instanceof RowsLogEvent) {
					if (logger.isDebugEnabled())
						logger.debug("got rows log event - event size = " + ((RowsLogEvent) logEvent).getEventSize());
					rowChangeData = new RowChangeData();
					RowsLogEvent rowsEvent = (RowsLogEvent) logEvent;
					TableMapLogEvent tableEvent = tableEvents.get(rowsEvent.getTableId());
					rowsEvent.processExtractedEvent(rowChangeData, tableEvent);
					dataArray.add(rowChangeData);
					foundRowsLogEvent = true;
				} else if (logEvent instanceof BeginLoadQueryLogEvent) {
					BeginLoadQueryLogEvent event = (BeginLoadQueryLogEvent) logEvent;
					if (prefetchSchemaNameLDI) {
						if (loadDataSchemas == null)
							loadDataSchemas = new HashMap<Integer, String>();
						loadDataSchemas.put(Integer.valueOf(event.getFileID()), event.getSchemaName());
					}
					dataArray.add(new LoadDataFileFragment(event.getFileID(), event.getData(), event.getSchemaName()));
					doFileFragment = true;
				} else if (logEvent instanceof AppendBlockLogEvent) {
					AppendBlockLogEvent event = (AppendBlockLogEvent) logEvent;
					String schema = null;
					if (prefetchSchemaNameLDI && loadDataSchemas != null)
						schema = loadDataSchemas.get(Integer.valueOf(event.getFileID()));
					dataArray.add(new LoadDataFileFragment(event.getFileID(), event.getData(), schema));
					doFileFragment = true;
				} else if (logEvent instanceof ExecuteLoadQueryLogEvent) {
					ExecuteLoadQueryLogEvent event = (ExecuteLoadQueryLogEvent) logEvent;
					if (loadDataSchemas != null)
						loadDataSchemas.remove(Integer.valueOf(event.getFileID()));
					String queryString = event.getQuery();
					LoadDataFileQuery statement = new LoadDataFileQuery(queryString, event.getWhen().getTime(), event.getDefaultDb(), event.getFileID(), event.getStartPos(), event.getEndPos());
					/* Adding statement options */
					statement.addOption("autocommit", event.getAutocommitFlag());
					statement.addOption("sql_auto_is_null", event.getAutoIsNullFlag());
					statement.addOption("foreign_key_checks", event.getForeignKeyChecksFlag());
					statement.addOption("unique_checks", event.getUniqueChecksFlag());

					/* Adding statement sql_mode */
					statement.addOption("sql_mode", event.getSqlMode());

					/* Adding character set / collation information */
					statement.addOption("character_set_client", String.valueOf(event.getClientCharsetId()));
					statement.addOption("collation_connection", String.valueOf(event.getClientCollationId()));
					statement.addOption("collation_server", String.valueOf(event.getServerCollationId()));

					// Extract the charset name if it can be found.
					String charsetName = event.getCharsetName();
					if (charsetName != null)
						statement.addOption(ReplOptionParams.JAVA_CHARSET_NAME, event.getCharsetName());

					if (logger.isDebugEnabled()) {
						logger.debug("statement.getOptions() = " + statement.getOptions());
					}
					statement.setErrorCode(event.getErrorCode());
					dataArray.add(statement);
					doFileFragment = true;
				} else if (logEvent instanceof DeleteFileLogEvent) {
					LoadDataFileDelete delete = new LoadDataFileDelete(((DeleteFileLogEvent) logEvent).getFileID());
					dataArray.add(delete);
				} else {
					logger.debug("got binlog event: " + logEvent);
				}

				if (doCommit || doRollback) {
					logger.debug("Performing commit processing in extractor");

					// runtime.getMonitor().incrementEvents(dataArray.size());
					String eventId = getDBMSEventId(position, sessionId);

					dbmsEvent = new DBMSEvent(eventId, dataArray, startTime);
					if (foundRowsLogEvent)
						dbmsEvent.setOptions(savedOptions);

					// Reset tableEvents hashtable when commit occurs
					logger.debug("Clearing Table Map events");
					tableEvents.clear();
					savedOptions.clear();
				} else if (transactionFragSize > 0 && fragSize > transactionFragSize) {
					if (logger.isDebugEnabled())
						logger.debug("Fragmenting -- fragment size = " + fragSize);
					// Extracted event size reached the fragmentation size =>
					// fragment event into several pieces.
					// Please note that consistency check events should not be
					// fragmented as they are not handled by the following code
					// runtime.getMonitor().incrementEvents(dataArray.size());
					String eventId = getDBMSEventId(position, sessionId);
					dbmsEvent = new DBMSEvent(eventId, dataArray, false, startTime);
					if (foundRowsLogEvent)
						dbmsEvent.setOptions(savedOptions);

					this.fragmentedTransaction = true;
				} else if (doFileFragment) {
					doFileFragment = false;
					// runtime.getMonitor().incrementEvents(dataArray.size());
					String eventId = getDBMSEventId(position, sessionId);
					dbmsEvent = new DBMSEvent(eventId, dataArray, false, startTime);
					if (foundRowsLogEvent)
						dbmsEvent.setOptions(savedOptions);

				}
				if (dbmsEvent != null) {
					dbmsEvent.addMetadataOption(ReplOptionParams.SERVER_ID, String.valueOf(serverId));
					if (doRollback)
						dbmsEvent.addMetadataOption(ReplOptionParams.ROLLBACK, "");
					if (unsafeForBlockCommit)
						dbmsEvent.addMetadataOption(ReplOptionParams.UNSAFE_FOR_BLOCK_COMMIT, "");
					return dbmsEvent;
				}
			}
		} catch (ExtractorException e) {
			if (runtime.getExtractorFailurePolicy() == FailurePolicy.STOP)
				throw new ExtractorException("Failed to extract from " + position, e);
			else
				logger.error("Failed to extract from " + position, e);

		} catch (InterruptedException e) {
			// We just pass this up the stack as we are being cancelled.
			throw e;
		} catch (Exception e) {
			if (runtime.getExtractorFailurePolicy() == FailurePolicy.STOP)
				throw new ExtractorException("Unexpected failure while extracting event " + position, e);
			else
				logger.error("Unexpected failure while extracting event " + position, e);

		}
		return null;
	}

	public synchronized DBMSEvent extract() throws InterruptedException, ReplicatorException {
		return extractEvent();
	}

	public DBMSEvent extract(String id) throws InterruptedException, ReplicatorException {
		setLastEventId(id);
		return extract();
	}

	public void setLastEventId(String eventId) throws ReplicatorException {
		if (eventId != null) {
			int colonIndex = eventId.indexOf(':');
			int semicolonIndex = eventId.indexOf(";");

			String binlogFileIndex = eventId.substring(0, colonIndex);

			long binlogOffset;

			if (semicolonIndex != -1) {
				binlogOffset = Long.valueOf(eventId.substring(colonIndex + 1, semicolonIndex));
			} else {
				binlogOffset = Long.valueOf(eventId.substring(colonIndex + 1));
			}

			logExtractor.initBinlogPosition(binlogFileIndex, binlogOffset);
		} else {
			logExtractor.initBinlogPosition(null, 0);
		}
	}

	public void configure(PluginContext context) throws ReplicatorException, InterruptedException {
		runtime = (ReplicatorRuntime) context;
		// Configure log event extractor.
		DirectLogExtractor directExtractor = new DirectLogExtractor(host, port, user, password);
		directExtractor.setBinlogFilePattern(this.binlogFilePattern);
		directExtractor.setFailurePolicy(this.extractorFailurePolicy);
		directExtractor.setCorePoolSize(this.corePoolSize);
		directExtractor.setMaximumPoolSize(this.maximumPoolSize);
		directExtractor.setServerId(this.serverId);
		directExtractor.setQueueCapacity(this.queueCapacity);

		logExtractor = directExtractor;
		if (logExtractor == null) {
			throw new ExtractorException("The log extractor is not configured or does not exist.");
		}

		// Make logextractor use the same useBytesForStrings.
		if (useBytesForStrings) {
			logExtractor.setUsingBytesForString(useBytesForStrings);
		}
	}

	public void prepare(PluginContext context) throws ReplicatorException, InterruptedException {
	}

	public void release(PluginContext context) throws ReplicatorException, InterruptedException {
		logExtractor.release();
	}

	public String getCurrentResourceEventId() throws ReplicatorException, InterruptedException {
		BinlogPosition position = logExtractor.getResourcePosition();
		String binlogFile = position.getFileName();
		long binlogOffset = position.getPosition();
		String eventId = binlogFile.substring(binlogFile.lastIndexOf('.') + 1) + ":" + getPositionAsString(binlogOffset);
		return eventId;
	}

	public void setPrefetchSchemaNameLDI(boolean prefetchSchemaNameLDI) {
		this.prefetchSchemaNameLDI = prefetchSchemaNameLDI;
	}
}
