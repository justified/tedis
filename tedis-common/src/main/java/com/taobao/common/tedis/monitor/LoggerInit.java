/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.monitor;

import java.io.File;
import java.util.Enumeration;

import org.apache.log4j.Appender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.xml.DOMConfigurator;


/**
 * logger初始化，把日志输出到应用的目录里
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-8-11 09:52:45
 * @version 1.0
 */
public class LoggerInit {
	public static final Logger TEDIS_LOG = Logger.getLogger("Tedis_LOG");
	public static final Logger logger = TEDIS_LOG;

	static private volatile boolean initOK = false;

	private static String getLogPath() {
		String userHome = System.getProperty("user.home");
		if (!userHome.endsWith(File.separator)) {
			userHome += File.separator;
		}
		String path = userHome + "logs" + File.separator + "tedis" + File.separator;
		File dir = new File(path);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		return path;
	}

	static {
		initTedisLog();
	}

	private static Appender buildAppender(String name, String fileName, String pattern) {
		DailyRollingFileAppender appender = new DailyRollingFileAppender();
		appender.setName(name);
		appender.setAppend(true);
		appender.setEncoding("GBK");
		appender.setLayout(new PatternLayout(pattern));
		appender.setFile(new File(getLogPath(), fileName).getAbsolutePath());
		appender.activateOptions();
		return appender;
	}

	static public void initTedisLog() {
		if (initOK)
			return;
		Appender statisticAppender = buildAppender("Tedis_Appender", "tedis.log", "%m");
		TEDIS_LOG.setAdditivity(false);
		TEDIS_LOG.removeAllAppenders();
		TEDIS_LOG.addAppender(statisticAppender);
		TEDIS_LOG.setLevel(Level.INFO);
	}

	static public void initTedisLogByFile() {
		if (initOK)
			return;

		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(LoggerInit.class.getClassLoader());
		DOMConfigurator.configure(LoggerInit.class.getClassLoader().getResource("tedis-log4j.xml"));

		String logPath = getLogPath();
		for (Enumeration<?> e = Logger.getLogger("only_for_get_all_appender").getAllAppenders(); e.hasMoreElements();) {
			Appender appender = (Appender) e.nextElement();
			if (FileAppender.class.isInstance(appender)) {
				FileAppender logFileAppender = (FileAppender) appender;
				File deleteFile = new File(logFileAppender.getFile());
				File logFile = new File(logPath, logFileAppender.getFile());
				logFileAppender.setFile(logFile.getAbsolutePath());
				logFileAppender.activateOptions();
				if (deleteFile.exists()) {
					deleteFile.delete();
				}
				logger.warn("成功添加日志" + deleteFile.getName() + "到" + logFile.getAbsolutePath());
			}
		}
		Thread.currentThread().setContextClassLoader(loader);
		initOK = true;
	}
}
