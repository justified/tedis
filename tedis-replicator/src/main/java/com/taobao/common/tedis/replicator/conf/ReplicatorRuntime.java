/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.conf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.taobao.common.tedis.replicator.PropertyException;
import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.ReplicatorProperties;
import com.taobao.common.tedis.replicator.manage.ReplicatorContext;
import com.taobao.common.tedis.replicator.pipeline.Pipeline;
import com.taobao.common.tedis.replicator.pipeline.Stage;
import com.taobao.common.tedis.replicator.plugin.PluginContext;
import com.taobao.common.tedis.replicator.plugin.PluginException;
import com.taobao.common.tedis.replicator.plugin.PluginLoader;
import com.taobao.common.tedis.replicator.plugin.PluginSpecification;
import com.taobao.common.tedis.replicator.plugin.ReplicatorPlugin;
import com.taobao.common.tedis.replicator.redis.RedisHandleException;
import com.taobao.common.tedis.replicator.redis.RedisHandler;
import com.taobao.common.tedis.replicator.redis.RedisHandlerLoader;
import com.taobao.common.tedis.replicator.statemachine.event.EventDispatcher;
import com.taobao.common.tedis.replicator.storage.Store;

public class ReplicatorRuntime implements PluginContext {
    private static Logger logger = Logger.getLogger(ReplicatorRuntime.class);

    private ReplicatorProperties properties;

    private Pipeline pipeline;

    private HashMap<String, RedisHandler> extensions = new HashMap<String, RedisHandler>();

    private ReplicatorContext context;

    private FailurePolicy extractorFailurePolicy;
    private FailurePolicy applierFailurePolicy;

    private boolean autoEnable;

    private String replicatorSchemaName;

    private String sourceId;

    private String serviceName;

    private boolean logSlaveUpdates = false;

    public ReplicatorRuntime(ReplicatorProperties properties, ReplicatorContext context) {
        this.properties = new ReplicatorProperties(properties.map());
        this.properties.trim();
        this.context = context;
    }

    public void configure() throws ReplicatorException {
        // Set auto-enable property.
        String autoEnableSetting = assertPropertyDefault(ReplicatorConf.AUTO_ENABLE, ReplicatorConf.AUTO_ENABLE_DEFAULT);
        autoEnable = new Boolean(autoEnableSetting);

        // Ensure source ID is available.
        sourceId = assertPropertyDefault(ReplicatorConf.SOURCE_ID, ReplicatorConf.SOURCE_ID_DEFAULT);

        // Ensure service name is available.
        serviceName = assertPropertySet(ReplicatorConf.SERVICE_NAME);
        MDC.put("serviceName", serviceName);

        // Store schema metadata. This default is necessary for unit tests to
        // work efficiently.
        this.replicatorSchemaName = assertPropertyDefault(ReplicatorConf.METADATA_SCHEMA, "tedis_" + serviceName);

        // See if we should log updates.
        assertPropertyDefault(ReplicatorConf.LOG_SLAVE_UPDATES, ReplicatorConf.LOG_SLAVE_UPDATES_DEFAULT);
        this.logSlaveUpdates = properties.getBoolean(ReplicatorConf.LOG_SLAVE_UPDATES);

        // Set default for resource JDBC URL so that unit tests run properly.
        // This value is normally set in the replicator properties.
        assertPropertyDefault(ReplicatorConf.RESOURCE_JDBC_URL, ReplicatorConf.RESOURCE_JDBC_URL_DEFAULT);

        // Set extractor failure policy.
        String extractorFailureSetting = assertPropertyDefault(ReplicatorConf.EXTRACTOR_FAILURE_POLICY, ReplicatorConf.EXTRACTOR_FAILURE_POLICY_DEFAULT);
        if (extractorFailureSetting.equals("stop")) {
            logger.info("Setting applierFailurePolicy to stop");
            extractorFailurePolicy = FailurePolicy.STOP;
        } else if (extractorFailureSetting.equals("warn")) {
            logger.info("Setting applierFailurePolicy to warn");
            extractorFailurePolicy = FailurePolicy.WARN;
        } else {
            throw new ReplicatorException("Valid values for " + ReplicatorConf.EXTRACTOR_FAILURE_POLICY + " are either 'stop' or 'skip'. Found: " + extractorFailureSetting);
        }

        // Set applier failure policy.
        String applierFailureSetting = assertPropertyDefault(ReplicatorConf.APPLIER_FAILURE_POLICY, ReplicatorConf.APPLIER_FAILURE_POLICY_DEFAULT);
        if (applierFailureSetting.equals("stop")) {
            logger.info("Setting applierFailurePolicy to stop");
            applierFailurePolicy = FailurePolicy.STOP;
        } else if (applierFailureSetting.equals("warn")) {
            logger.info("Setting applierFailurePolicy to warn");
            applierFailurePolicy = FailurePolicy.WARN;
        } else {
            throw new ReplicatorException("Valid values for " + ReplicatorConf.APPLIER_FAILURE_POLICY + " are either 'stop' or 'skip'. Found: "
                    + properties.getString(ReplicatorConf.APPLIER_FAILURE_POLICY));

        }

        // Instantiate and configure extensions.
        instantiateExtensions();

        // Configure the pipeline.
        String pipeline = assertPropertySet(ReplicatorConf.PIPELINE);
        if (!pipeline.isEmpty()) {
            logger.info("Setting pipeline name to " + pipeline);
        }
        instantiateAndConfigurePipeline(pipeline);
    }

    protected void instantiateExtensions() throws ReplicatorException {
        List<String> extensionNames = properties.getStringList(ReplicatorConf.EXTENSIONS);
        for (String extensionName : extensionNames) {
            RedisHandler extension = loadAndConfigureRedisHandler(ReplicatorConf.EXTENSION_ROOT, extensionName);
            configurePlugin(extension, this);
            extensions.put(extensionName, extension);
        }
    }

    protected void instantiateAndConfigurePipeline(String name) throws ReplicatorException {
        // Instantiate pipeline.
        Pipeline newPipeline = new Pipeline();
        newPipeline.setName(name);

        boolean autoSync = false;
        String autoSyncProperty = ReplicatorConf.PIPELINE_ROOT + "." + name + ".autoSync";
        if (properties.get(autoSyncProperty) != null)
            autoSync = properties.getBoolean(autoSyncProperty);

        newPipeline.setAutoSync(autoSync);

        // Add stores, if any.
        String storesProperty = ReplicatorConf.PIPELINE_ROOT + "." + name + ".stores";
        List<String> stores = properties.getStringList(storesProperty);

        for (String storeName : stores) {
            Store store = (Store) loadAndConfigurePlugin(ReplicatorConf.STORE_ROOT, storeName);
            store.setName(storeName);
            newPipeline.addStore(storeName, store);
        }

        // Add stages.
        String stagesProperty = ReplicatorConf.PIPELINE_ROOT + "." + name;
        List<String> stages = properties.getStringList(stagesProperty);

        if (stages.size() == 0) {
            throw new ReplicatorException("Pipeline does not exist or has no stages: " + name);
        }

        for (String stageName : stages) {
            String stageProperty = ReplicatorConf.STAGE_ROOT + "." + stageName + ".";
            ReplicatorProperties stageProps = properties.subset(stageProperty, true);

            // Instantiate a stage.
            Stage stage = new Stage(newPipeline);
            stage.setName(stageName);
            newPipeline.addStage(stage);

            // Find and load extractor.
            String extractorName = stageProps.remove(ReplicatorConf.EXTRACTOR);
            if (extractorName == null) {
                throw new ReplicatorException("No extractor specified for stage: " + stageName);
            } else {
                // Load extractor.
                PluginSpecification extractorSpecification = specifyPlugin(ReplicatorConf.EXTRACTOR_ROOT, extractorName);
                stage.setExtractorSpec(extractorSpecification);
            }

            // Find and load filters.
            List<String> filterNames = stageProps.getStringList(ReplicatorConf.FILTERS);
            stageProps.remove(ReplicatorConf.FILTERS);
            List<PluginSpecification> filterSpecs = new ArrayList<PluginSpecification>();
            for (String filterName : filterNames) {
                PluginSpecification fps = specifyPlugin(ReplicatorConf.FILTER_ROOT, filterName);
                filterSpecs.add(fps);
            }
            stage.setFilterSpecs(filterSpecs);

            // Find and load extractor.
            String applierName = stageProps.remove(ReplicatorConf.APPLIER);
            if (applierName == null) {
                throw new ReplicatorException("No applier specified for stage: " + stageName);
            } else {
                PluginSpecification applierSpec = specifyPlugin(ReplicatorConf.APPLIER_ROOT, applierName);
                stage.setApplierSpec(applierSpec);
            }

            // Any remaining properties should be applied to the stage instance.
            stageProps.applyProperties(stage);
        }

        // Configure the pipeline and then make it visible in the runtime.
        try {
            newPipeline.configure(this);
        } catch (InterruptedException e) {
            // We are not really ready to handle an interruption in a
            // civilized way so we just die.
            throw new ReplicatorException("Pipeline configuration was interrupted");
        }
        pipeline = newPipeline;
    }

    public void prepare() throws ReplicatorException, InterruptedException {
        for (String extensionName : extensions.keySet()) {
            logger.info("Preparing extension service for use: " + extensionName);
            extensions.get(extensionName).prepare(this);
        }
        logger.info("Preparing pipeline for use: " + pipeline.getName());
        pipeline.prepare(this);
    }

    public void release() {
        if (pipeline != null) {
            pipeline.release(this);
            pipeline = null;
        }
    }

    protected String assertPropertyDefault(String key, String value) {
        if (properties.getString(key) == null) {
            if (logger.isDebugEnabled())
                logger.debug("Assigning default global property value: key=" + key + " default value=" + value);
            properties.setString(key, value);
        }
        return properties.getString(key);
    }

    protected String assertPropertySet(String key) throws ReplicatorException {
        String value = properties.getString(key);
        if (value == null)
            throw new ReplicatorException("Required property not set: key=" + key);
        else
            return value;
    }

    private RedisHandler loadAndConfigureRedisHandler(String prefix, String name) throws ReplicatorException {
        String pluginPrefix = prefix + "." + name.trim();
        String pluginClassName = properties.getString(pluginPrefix);
        if (logger.isDebugEnabled())
            logger.debug("Loading plugin: key=" + pluginPrefix + " class name=" + pluginClassName);
        ReplicatorProperties pluginProperties = properties.subset(pluginPrefix + ".", true);
        if (logger.isDebugEnabled())
            logger.debug("Plugin properties: " + pluginProperties.toString());

        RedisHandler plugin;
        try {
            plugin = new RedisHandlerLoader(serviceName).load(pluginClassName);
            pluginProperties.applyProperties(plugin);
        } catch (RedisHandleException e) {
            throw new ReplicatorException("Unable to load plugin class: key=" + pluginPrefix + " class name=" + pluginClassName, e);
        } catch (PropertyException e) {
            throw new ReplicatorException("Unable to configure plugin properties: key=" + pluginPrefix + " class name=" + pluginClassName + " : " + e.getMessage(), e);
        }
        return plugin;
    }

    public ReplicatorPlugin loadAndConfigurePlugin(String prefix, String name) throws ReplicatorException {
        String pluginPrefix = prefix + "." + name.trim();

        // Find the plug-in class name.
        String rawClassName = properties.getString(pluginPrefix);
        if (rawClassName == null)
            throw new ReplicatorException("Plugin class name property is missing or null:  key=" + pluginPrefix);
        String pluginClassName = rawClassName.trim();
        if (logger.isDebugEnabled())
            logger.debug("Loading plugin: key=" + pluginPrefix + " class name=" + pluginClassName);

        // Subset plug-in properties.
        ReplicatorProperties pluginProperties = properties.subset(pluginPrefix + ".", true);
        if (logger.isDebugEnabled())
            logger.debug("Plugin properties: " + pluginProperties.toString());

        // Load the plug-in class and configure its properties.
        ReplicatorPlugin plugin;
        try {
            plugin = PluginLoader.load(pluginClassName);
            pluginProperties.applyProperties(plugin);
        } catch (PluginException e) {
            throw new ReplicatorException("Unable to load plugin class: key=" + pluginPrefix + " class name=" + pluginClassName, e);
        } catch (PropertyException e) {
            throw new ReplicatorException("Unable to configure plugin properties: key=" + pluginPrefix + " class name=" + pluginClassName + " : " + e.getMessage(), e);
        }
        return plugin;
    }

    protected PluginSpecification specifyPlugin(String prefix, String name) throws ReplicatorException {
        String pluginPrefix = prefix + "." + name.trim();

        // Find the plug-in class name.
        String rawClassName = properties.getString(pluginPrefix);
        if (rawClassName == null)
            throw new ReplicatorException("Plugin class name property is missing or null:  key=" + pluginPrefix);
        String pluginClassName = rawClassName.trim();
        if (logger.isDebugEnabled())
            logger.debug("Loading plugin: key=" + pluginPrefix + " class name=" + pluginClassName);

        // Subset plug-in properties.
        ReplicatorProperties pluginProperties = properties.subset(pluginPrefix + ".", true);
        if (logger.isDebugEnabled())
            logger.debug("Plugin properties: " + pluginProperties.toString());

        // Load the plug-in class and configure its properties.
        Class<?> pluginClass;
        try {
            pluginClass = PluginLoader.loadClass(pluginClassName);
        } catch (PluginException e) {
            throw new ReplicatorException("Unable to instantiate plugin class: key=" + pluginPrefix + " class name=" + pluginClassName, e);
        }

        return new PluginSpecification(pluginPrefix, name, pluginClass, pluginProperties);
    }

    public ReplicatorProperties getReplicatorProperties() {
        return properties;
    }

    public String getJdbcUrl(String database) {
        Properties jProps = new Properties();
        jProps.setProperty("URL", properties.getString(ReplicatorConf.RESOURCE_JDBC_URL));
        if (database == null)
            jProps.setProperty("DBNAME", this.getReplicatorSchemaName());
        else
            jProps.setProperty("DBNAME", database);
        ReplicatorProperties.substituteSystemValues(jProps);
        return jProps.getProperty("URL");
    }

    public String getJdbcUser() {
        return properties.getString(ReplicatorConf.GLOBAL_DB_USER);
    }

    public String getJdbcPassword() {
        return properties.getString(ReplicatorConf.GLOBAL_DB_PASSWORD);
    }

    public String getReplicatorSchemaName() {
        return replicatorSchemaName;
    }

    public String getSourceId() {
        return sourceId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public boolean isAutoEnable() {
        return autoEnable;
    }

    public Store getStore(String name) {
        return pipeline.getStore(name);
    }

    public List<Store> getStores() {
        ArrayList<Store> stores = new ArrayList<Store>();
        for (String name : pipeline.getStoreNames())
            stores.add(pipeline.getStore(name));
        return stores;
    }

    public EventDispatcher getEventDispatcher() {
        return context.getEventDispatcher();
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public FailurePolicy getApplierFailurePolicy() {
        return applierFailurePolicy;
    }

    public FailurePolicy getExtractorFailurePolicy() {
        return extractorFailurePolicy;
    }

    public long getMinSeqno() {
        return pipeline.getMinStoredSeqno();
    }

    public long getMaxSeqno() {
        return pipeline.getMaxStoredSeqno();
    }

    public ReplicatorPlugin getExtension(String name) {
        return extensions.get(name);
    }

    public List<String> getExtensionNames() {
        return new ArrayList<String>(extensions.keySet());
    }

    public static void configurePlugin(ReplicatorPlugin plugin, PluginContext context) throws ReplicatorException {
        String pluginClassName = plugin.getClass().getName();
        try {
            plugin.configure(context);
        } catch (ReplicatorException e) {
            throw new ReplicatorException("Unable to configure plugin: class name=" + pluginClassName, e);
        } catch (Throwable t) {
            String message = "Unable to configure plugin: class name=" + pluginClassName;

            logger.error(message, t);
            throw new ReplicatorException(message, t);
        }

        // It worked. We have a configured plugin.
        if (logger.isDebugEnabled())
            logger.debug("Plug-in configured successfully: class name=" + pluginClassName);
    }

    public static void preparePlugin(ReplicatorPlugin plugin, PluginContext context) throws ReplicatorException {
        String pluginClassName = plugin.getClass().getName();
        try {
            plugin.prepare(context);
        } catch (ReplicatorException e) {
            throw new ReplicatorException("Unable to prepare plugin: class name=" + pluginClassName, e);
        } catch (Throwable t) {
            String message = "Unable to prepare plugin: class name=" + pluginClassName;

            logger.error(message, t);
            throw new ReplicatorException(message, t);
        }

        // It worked. We have a prepared plugin.
        if (logger.isDebugEnabled())
            logger.debug("Plug-in prepared successfully: class name=" + pluginClassName);
    }

    public static void releasePlugin(ReplicatorPlugin plugin, PluginContext context) {
        String pluginClassName = plugin.getClass().getName();
        try {
            plugin.release(context);
        } catch (Throwable t) {
            logger.warn("Unable to release plugin: class name=" + pluginClassName, t);
        }

        if (logger.isDebugEnabled())
            logger.debug("Plug-in released successfully: class name=" + pluginClassName);
    }

    public boolean logReplicatorUpdates() {
        return logSlaveUpdates;
    }

}