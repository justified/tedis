/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor.mysql;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.conf.FailurePolicy;
import com.taobao.common.tedis.replicator.database.Database;
import com.taobao.common.tedis.replicator.database.DatabaseFactory;
import com.taobao.common.tedis.replicator.extractor.ExtractorException;
import com.taobao.common.tedis.replicator.extractor.mysql.conversion.LittleEndianConversion;
import com.taobao.common.tedis.replicator.mysql.MySQLConstants;
import com.taobao.common.tedis.replicator.mysql.MySQLPacket;

public final class DirectLogExtractor implements LogExtractor {
	private static Logger logger = Logger.getLogger(DirectLogExtractor.class);

	private String host = "localhost";
	private int port = 3306;
	private String user = "root";
	private String password = "";
	private boolean parseStatements = true;

	private String binlogFilePattern = "mysql-bin";
	private int serverId = 25;

	private String url;

	// Database connection information.
	private Connection conn;
	private InputStream input = null;
	private OutputStream output = null;
	private int readRetry = 20;

	private Thread binlogThread = null;
	private BinlogPosition binlogPosition = null;

	// Number of millisecionds to wait before checking log index for a missing
	// log-rotate event.
	private long millsTimeoutInterval = 5000;

	// Varchar type fields can be retrieved and stored in THL either using
	// String datatype or bytes arrays. By default, using string datatype.
	private boolean useBytesForStrings = false;

	// The event queue capacity.
	private int queueCapacity = 8192;
	private LinkedBlockingQueue<EventAndPosition> eventQueue = null;

	// The executor pool sizes.
	private boolean useThreadPool = false;
	private int corePoolSize = 8;
	private int maximumPoolSize = 8;
	private long keepAliveTime = 0L;
	private ExecutorService executor = null;

	private FailurePolicy extractorFailurePolicy;

	public EventAndPosition extractLogEvent() throws ExtractorException, InterruptedException {
		if (binlogThread == null || !binlogThread.isAlive())
			startLogEvents(binlogPosition);

		return eventQueue.poll(millsTimeoutInterval, TimeUnit.MILLISECONDS);
	}

	private void startLogEvents(BinlogPosition binlogPosition) throws InterruptedException, MySQLExtractException {
		// Shutdown the log event first.
		stopLogEvents();

		// Setting binlog position & open input stream
		this.binlogPosition = binlogPosition;

		if (useThreadPool) {
			// Init the executor thread pool.
			executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		}

		// Start the log event extracting thread
		binlogThread = new Thread(new ProcessEventTask(), "Log Extracting Thread");
		binlogThread.start();
	}

	private void stopLogEvents() throws InterruptedException {
		// Shutdown the log event extracting thread.
		if (binlogThread != null) {
			if (input != null) {
				try {
					input.close();
					input = null;
				} catch (IOException e) {
					// ignore
				}
			}
			binlogThread.interrupt();
			binlogThread.join();
			binlogThread = null;
		}

		// Shutdown the executor thread pool.
		if (executor != null) {
			executor.shutdown();
			if (!executor.isTerminated())
				executor.awaitTermination(millsTimeoutInterval, TimeUnit.MILLISECONDS);
			executor = null;
		}
	}

	private final class ProcessEventTask implements Runnable {
		public void run() {
			if (logger.isDebugEnabled()) {
				logger.debug("Log event extracting task starting...");
			}

			try {
				connect();

				while (!Thread.interrupted()) {
					try {
						EventAndPosition event = processEvent();

						if (logger.isDebugEnabled()) {
							BinlogPosition position = event.getPosition();
							logger.debug("Extracting from pos, file: " + position.getFileName() + " pos: " + position.getPosition());
						}

						eventQueue.put(event);
					} catch (MySQLExtractException e) {
						// Put exception event into queue.
						eventQueue.put(new EventAndPosition(binlogPosition, e));

						logger.warn("Extracting error", e);

						// Exit if extractor failure policy is STOP.
						if (extractorFailurePolicy == FailurePolicy.STOP)
							break;
					}
				}
			} catch (InterruptedException e) {
				logger.info("Log event extracting task interrupted.");
			} catch (Throwable e) {
				logger.error("Log event extracting error.", e);
			} finally {
				disconnect();
			}

			if (logger.isDebugEnabled()) {
				logger.debug("Log event extracting task stopped.");
			}
		}
	}

	private void connect() throws ExtractorException {
		try {
			logger.info("Connecting to master MySQL server: host=" + host + " port=" + port);
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port, user, password);
		} catch (ClassNotFoundException e) {
			throw new ExtractorException("Unable to load JDBC driver", e);
		} catch (SQLException e) {
			throw new ExtractorException("Unable to connect", e);
		}

		// Get underlying IO streams for network communications.
		try {
			Object io = this.getMysqlConnectionIO(conn);
			input = this.getMysqlConnectionInputStream(io);
			output = this.getMysqlConnectionOutputStream(io);
		} catch (Exception e) {
			throw new ExtractorException("Unable to access IO streams for connection", e);
		}

		// Ask for binlog data.
		try {
			logger.info("Requesting binlog data from master: " + binlogPosition.getFileName() + ":" + binlogPosition.getPosition());
			sendBinlogDumpPacket(output);
		} catch (IOException e) {
			throw new ExtractorException("Error sending request to dump binlog", e);
		}
	}

	private Object getMysqlConnectionIO(Connection conn) throws IOException {
		try {
			Class<?> clazz = null;
			String realName = conn.getClass().getName();
			if (realName.equals("com.mysql.jdbc.JDBC4Connection")) {
				clazz = Class.forName("com.mysql.jdbc.ConnectionImpl");
			} else {
				clazz = conn.getClass();
			}

			Field ioField = clazz.getDeclaredField("io");
			ioField.setAccessible(true);
			return ioField.get(conn);
		} catch (Exception e) {
			throw new IOException(String.format("Could not find field 'io' in class '%s'\n." + "Check to be sure that you have this class on your classPath", conn.getClass()));
		}
	}

	private InputStream getMysqlConnectionInputStream(Object io) throws IOException {
		try {
			Field isField = io.getClass().getDeclaredField("mysqlInput");
			isField.setAccessible(true);
			return (InputStream) isField.get(io);
		} catch (Exception e) {
			throw new IOException(e.getLocalizedMessage());
		}
	}

	private OutputStream getMysqlConnectionOutputStream(Object io) throws IOException {
		try {
			Field isField = io.getClass().getDeclaredField("mysqlOutput");
			isField.setAccessible(true);
			return (OutputStream) isField.get(io);
		} catch (Exception e) {
			throw new IOException(e.getLocalizedMessage());
		}
	}

	private void sendBinlogDumpPacket(OutputStream out) throws IOException {
		MySQLPacket packet = new MySQLPacket(200, (byte) 0);
		packet.putByte((byte) MySQLConstants.COM_BINLOG_DUMP);
		packet.putInt32((int) binlogPosition.getPosition());
		packet.putInt16(0);
		packet.putInt32(serverId); // MySql server id
		if (binlogPosition.getFileName() != null)
			packet.putString(binlogPosition.getFileName());
		packet.write(out);
		out.flush();
	}

	private void disconnect() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				logger.warn("Unable to close connection", e);
			}
		}
	}

	private EventAndPosition processEvent() throws InterruptedException, ReplicatorException {
		MySQLPacket packet = MySQLPacket.readPacket(input);
		if (packet == null) {
			// Retry while we get nothing from readPacket.
			for (int retry = 0; retry < readRetry && packet == null; retry++) {
				logger.info("Null packet, wait 10ms and retry");
				Thread.sleep(10);
				packet = MySQLPacket.readPacket(input);
			}

			if (packet == null) {
				throw new ExtractorException("No packet read, reached end of input stream.");
			}
		}

		int length = packet.getDataLength();
		int number = packet.getPacketNumber();
		short type = packet.getUnsignedByte();

		if (logger.isDebugEnabled()) {
			logger.debug("Received packet: number=" + number + " length=" + length + " type=" + type);
		}

		// Switch on the type.
		switch (type) {
		case 0:
			// Indicates the next event. (MySQL dev wiki doc on connection
			// protocol are wrong or misleading.)
			try {
				return processBinlogEvent(packet);
			} catch (IOException e) {
				throw new ExtractorException("Error processing binlog: " + e.getMessage(), e);
			}

		case 0xFE:
			// Indicates end of stream. It's not clear when this would
			// be sent.
			throw new ExtractorException("EOF packet received");
		case 0xFF:
			// Indicates an error, for example trying to restart at wrong
			// binlog offset.
			int errno = packet.getShort();
			packet.getByte();
			String sqlState = packet.getString(5);
			String errMsg = packet.getString();

			String msg = "Error packet received: errno=" + errno + " sqlstate=" + sqlState + " error=" + errMsg;
			throw new ExtractorException(msg);
		default:
			// Should not happen.
			throw new ExtractorException("Unexpected response while fetching binlog data: packet=" + packet.toString());
		}
	}

	private EventAndPosition processBinlogEvent(MySQLPacket packet) throws IOException, ReplicatorException {
		// Read the header. Note we can only handle V4 headers (5.0+).
		long timestamp = packet.getUnsignedInt32();
		int typeCode = packet.getUnsignedByte();
		long serverId = packet.getUnsignedInt32();
		long eventLength = packet.getUnsignedInt32();
		long nextPosition = packet.getUnsignedInt32();
		int flags = packet.getUnsignedShort();

		if (logger.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder("Reading binlog event:");
			sb.append(" timestamp=").append(timestamp);
			sb.append(" type_code=").append(typeCode);
			sb.append(" server_id=").append(serverId);
			sb.append(" event_length=").append(eventLength);
			sb.append(" next_position=").append(nextPosition);
			sb.append(" flags=").append(flags);
			logger.debug(sb.toString());
		}

		// Read full event to buffer
		ByteBuffer buffer = ByteBuffer.allocate((int) eventLength);
		writePacketToRelayLog(buffer, packet);
		final byte[] fullEvent = buffer.array();

		FormatDescriptionLogEvent description_event = new FormatDescriptionLogEvent(4);
		if (binlogPosition.getPosition() == 0) {
			/* always test for a Start_v3, even if no --start-position */
			if (typeCode == MysqlBinlog.START_EVENT_V3) {
				/* This is 3.23 or 4.x */
				if (LittleEndianConversion.convert4BytesToLong(fullEvent, MysqlBinlog.EVENT_LEN_OFFSET) < (MysqlBinlog.LOG_EVENT_MINIMAL_HEADER_LEN + MysqlBinlog.START_V3_HEADER_LEN)) {
					/* This is 3.23 (format 1) */
					description_event = new FormatDescriptionLogEvent(1);
				}
			} else if (typeCode == MysqlBinlog.FORMAT_DESCRIPTION_EVENT) {
				/* This is 5.0 */
				FormatDescriptionLogEvent new_description_event;
				new_description_event = (FormatDescriptionLogEvent) LogEvent.readLogEvent(parseStatements, fullEvent, fullEvent.length, description_event, useBytesForStrings);
				if (new_description_event == null)
				/* EOF can't be hit here normally, so it's a real error */
				{
					logger.error("Could not read a format_description_log_event event, " + "this could be a log format error or read error");
					throw new MySQLExtractException("binlog format error");
				}
				description_event = new_description_event;
				logger.debug("Setting description_event");
			}
		}

		if (nextPosition != 0) {
			/* update binlog position up to where we read */
			binlogPosition.setPosition(nextPosition);
		}

		if (typeCode == MysqlBinlog.ROTATE_EVENT) {
			if (logger.isDebugEnabled()) {
				StringBuilder sb2 = new StringBuilder("ROTATE_EVENT:");
				sb2.append(" next_start_offset=").append(packet.getLong());
				sb2.append(" next_binlog_name=").append(packet.getString());
				logger.debug(sb2.toString());
			}

			RotateLogEvent event = new RotateLogEvent(fullEvent, fullEvent.length, description_event);

			EventAndPosition eventAndPosition = new EventAndPosition(binlogPosition.clone(), event);

			/* need to rotate to next binlog file */
			binlogPosition.setFileName(event.getNewBinlogFilename());

			// Return log event directly
			return eventAndPosition;
		}

		if (useThreadPool) {
			// Execute event extract in another threads.
			final FormatDescriptionLogEvent description = description_event;
			Future<LogEvent> future = executor.submit(new Callable<LogEvent>() {
				public LogEvent call() throws Exception {
					return LogEvent.readLogEvent(parseStatements, fullEvent, fullEvent.length, description, useBytesForStrings);
				}
			});

			return new EventAndPositionFuture(binlogPosition.clone(), future);
		} else {
			LogEvent logEvent = LogEvent.readLogEvent(parseStatements, fullEvent, fullEvent.length, description_event, useBytesForStrings);
			return new EventAndPosition(binlogPosition.clone(), logEvent);
		}
	}

	// Write a packet to relay log.
	private void writePacketToRelayLog(ByteBuffer buffer, MySQLPacket packet) throws IOException {
		blindlyWriteToRelayLog(buffer, packet, false);

		while (packet.getDataLength() >= MySQLPacket.MAX_LENGTH) {
			// this is a packet longer than 16m. Data will be send over several
			// packets so we need to read/write the next packets blindly until a
			// packet smaller than 16m is found
			packet = MySQLPacket.readPacket(input);
			if (logger.isDebugEnabled()) {
				logger.debug("Read extended packet: number=" + packet.getPacketNumber() + " length=" + packet.getDataLength());
			}
			blindlyWriteToRelayLog(buffer, packet, true);
		}
	}

	private void blindlyWriteToRelayLog(ByteBuffer buffer, MySQLPacket packet, boolean extended) throws IOException {
		byte[] bytes = packet.getByteBuffer();
		int header;
		// Header size affects math for start and length of written data.
		if (extended)
			header = 4;
		else
			header = 5;
		int writeLength = bytes.length - header;
		if (logger.isDebugEnabled()) {
			logger.debug("Writing packet to buffered binlog: bytesLength=" + bytes.length + " writeLength=" + writeLength);
		}
		buffer.put(bytes, header, writeLength);
	}

	public DirectLogExtractor(String host, int port, String user, String password) throws ReplicatorException {
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		// Compute our MySQL dbms URL.
		StringBuilder sb = new StringBuilder();
		sb.append("jdbc:mysql://");
		sb.append(host);
		sb.append(":");
		sb.append(port);
		sb.append("/");
		url = sb.toString();

		eventQueue = new LinkedBlockingQueue<EventAndPosition>(queueCapacity);
		// Configure log event extractor
		setKeepAliveTime((getCorePoolSize() == getMaximumPoolSize()) ? 0L : 60L);
	}

	public void setFailurePolicy(FailurePolicy failurePolicy) {
		// Fetch extractor failure policy.
		this.extractorFailurePolicy = failurePolicy;
	}

	public void release() throws ReplicatorException, InterruptedException {
		stopLogEvents();

		// Cleanup the event queue.
		if (eventQueue != null) {
			eventQueue.clear();
			eventQueue = null;
		}
	}

	public void setBinlogFile(String binlogFileName) throws MySQLExtractException, InterruptedException {
		// Do nothing.
	}

	public void nextBinlogFile(BinlogPosition position) throws ExtractorException, InterruptedException {
		// Restart log event extracting task.
		startLogEvents(position);
	}

	public void initBinlogPosition(String binlogFileIndex, long binlogOffset) throws ExtractorException {
		if (binlogFileIndex != null) {
			// We tolerate the event ID with or without the binlog prefix.
			String binlogFile;
			if (binlogFileIndex.startsWith(binlogFilePattern))
				binlogFile = binlogFileIndex;
			else
				binlogFile = binlogFilePattern + "." + binlogFileIndex;

			// Set the binlog position.
			binlogPosition = new BinlogPosition(binlogOffset, binlogFile, null);
		} else {
			binlogPosition = positionBinlog(null, true);
		}

		try {
			// Restart log event extracting task.
			startLogEvents(binlogPosition);
		} catch (InterruptedException e) {
			logger.warn("Starting log event extracting thread interrupted", e);
		}
	}

	private BinlogPosition positionBinlog(BinlogPosition position, boolean flush) throws ExtractorException {
		// Find current position binlog from master db.
		BinlogPosition nextPosition = positionBinlogFromDatabase(position, flush);

		logger.info("Starting from position: " + nextPosition.getFileName() + ":" + nextPosition.getPosition());
		return nextPosition;
	}

	private BinlogPosition positionBinlogFromDatabase(BinlogPosition position, boolean flush) throws ExtractorException, MySQLExtractException {
		Database database = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			database = DatabaseFactory.createDatabase(url, user, password);
			database.connect(true);
			st = database.createStatement();
			if (flush) {
				logger.debug("Flushing logs");
				st.executeUpdate("FLUSH LOGS");
			}
			logger.debug("Seeking head position in binlog");
			rs = st.executeQuery("SHOW MASTER STATUS");
			if (!rs.next())
				throw new ExtractorException("Error getting master status; is the MySQL binlog enabled?");
			String binlogFile = rs.getString(1);
			long binlogOffset;

			if (position == null || position.getFileName().equals(binlogFile)) {
				binlogOffset = rs.getLong(2);
			} else {
				// if binlog file changes, begin from the start of the new file
				binlogOffset = 0;
			}

			return new BinlogPosition(binlogOffset, binlogFile, null);
		} catch (SQLException e) {
			logger.info("url: " + url + " user: " + user + " password: ********");
			throw new ExtractorException(e);
		} finally {
			cleanUpDatabaseResources(database, st, rs);
		}
	}

	private void cleanUpDatabaseResources(Database conn, Statement st, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException ignore) {
			}
		}
		if (st != null) {
			try {
				st.close();
			} catch (SQLException ignore) {
			}
		}
		if (conn != null)
			conn.close();
	}

	public BinlogPosition getResourcePosition() throws ExtractorException, InterruptedException {
		// Find the last event ID from master db.
		return getLastPositionFromDatabase();
	}

	private BinlogPosition getLastPositionFromDatabase() throws ExtractorException {
		Database conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			conn = DatabaseFactory.createDatabase(url, user, password);
			conn.connect();
			st = conn.createStatement();
			logger.debug("Seeking head position in binlog");
			rs = st.executeQuery("SHOW MASTER STATUS");
			if (!rs.next())
				throw new ExtractorException("Error getting master status; is the MySQL binlog enabled?");
			String binlogFile = rs.getString(1);
			long binlogOffset = rs.getLong(2);

			return new BinlogPosition(binlogOffset, binlogFile, null);
		} catch (SQLException e) {
			logger.info("url: " + url + " user: " + user + " password: ********");
			throw new ExtractorException("Unable to run SHOW MASTER STATUS to find log position", e);
		} finally {
			cleanUpDatabaseResources(conn, st, rs);
		}
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

	public String getBinlogFilePattern() {
		return binlogFilePattern;
	}

	public void setBinlogFilePattern(String binlogFilePattern) {
		this.binlogFilePattern = binlogFilePattern;
	}

	public boolean isParseStatements() {
		return parseStatements;
	}

	public void setParseStatements(boolean parseStatements) {
		this.parseStatements = parseStatements;
	}

	public boolean isUsingBytesForString() {
		return useBytesForStrings;
	}

	public void setUsingBytesForString(boolean useBytes) {
		this.useBytesForStrings = useBytes;
	}

	public int getServerId() {
		return serverId;
	}

	public void setServerId(int serverId) {
		this.serverId = serverId;
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

	public long getKeepAliveTime() {
		return keepAliveTime;
	}

	public void setKeepAliveTime(long keepAliveTime) {
		this.keepAliveTime = keepAliveTime;
	}

	public void setMillsTimeoutInterval(long millsTimeoutInterval) {
		this.millsTimeoutInterval = millsTimeoutInterval;
	}
}