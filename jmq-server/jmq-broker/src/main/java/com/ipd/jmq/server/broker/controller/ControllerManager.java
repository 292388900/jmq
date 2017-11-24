package com.ipd.jmq.server.broker.controller;

import com.alibaba.fastjson.JSON;
import com.ipd.jmq.common.model.AppPermission;
import com.ipd.jmq.server.broker.context.BrokerContext;
import com.ipd.jmq.server.context.ContextEvent;
import com.ipd.jmq.registry.PathData;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryException;
import com.ipd.jmq.registry.listener.PathEvent;
import com.ipd.jmq.registry.listener.PathListener;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.config.Context;
import com.ipd.jmq.toolkit.io.Compressors;
import com.ipd.jmq.toolkit.io.Zip;
import com.ipd.jmq.toolkit.lang.Charsets;
import com.ipd.jmq.toolkit.service.Service;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


/**
 * 控制信息管理器
 */
public class ControllerManager extends Service {
    private static final Logger logger = LoggerFactory.getLogger(ControllerManager.class);

    private static final String SPECIAL_SPLIT_STRING = "@";
    // 系统控制
    private Context controls = new Context(null);
    // 上次控制系统配置uuid
    private Set<String> lastControllerUUIDs = new HashSet<String>();
    // 当前控制系统配置uuid
    private Set<String> currentControllerUUIDs = new HashSet<String>();
    // 控制信息监听器
    private PathListener controllerListener = new ControllerListener();
    // 事件派发器
    private EventBus<ContextEvent> eventBus;
    // 控制信息路径
    private String controllerPath = "/jmq/controller_zip";
    // 节点是否压缩
    private boolean registryCompressed = true;
    // 注册中心
    private Registry registry;

    public ControllerManager(Registry registry) {
        this(null, registry, true);
    }

    public ControllerManager(Registry registry, boolean registryCompressed) {
        this(null, registry, registryCompressed);
    }

    public ControllerManager(EventBus<ContextEvent> eventBus, Registry registry, boolean registryCompressed) {
        this.eventBus = eventBus;
        this.registryCompressed = registryCompressed;
        this.registry = registry;
    }

    @Override
    protected void validate() throws Exception {
        if (registry == null) {
            throw new IllegalArgumentException("registry can not be null");
        }
        if (eventBus == null) {
            eventBus = new EventBus<ContextEvent>("ctrlContextManager");
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        eventBus.start();
        if (registry.isConnected()) {
            updateControllerInfo();
        }

        registry.addListener(controllerPath, controllerListener);
        logger.info("controller context manager is started");
    }

    @Override
    protected void doStop() {
        super.doStop();
        registry.removeListener(controllerPath, controllerListener);
        eventBus.stop();
        controls.remove();
        lastControllerUUIDs.clear();
        currentControllerUUIDs.clear();
        logger.info("controller context manager is stopped");
    }

    /**
     * 添加监听器
     *
     * @param listener 监听器
     */
    public void addListener(EventListener<ContextEvent> listener) {
        if (eventBus.addListener(listener)) {
            if (isStarted()) {
                Iterator<Map.Entry<String, Object>> iterator = controls.iterator();
                Map.Entry<String, Object> entry;
                while (iterator.hasNext()) {
                    entry = iterator.next();
                    eventBus.add(new ContextEvent(entry.getKey(), (String) entry.getValue()), listener);
                }
            }
        }
    }

    /**
     * 移除监听器
     *
     * @param listener 监听器
     */
    public void removeListener(EventListener<ContextEvent> listener) {
        eventBus.removeListener(listener);
    }


    public Registry getRegistry() {
        return registry;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public boolean isRegistryCompressed() {
        return registryCompressed;
    }

    public void setRegistryCompressed(boolean registryCompressed) {
        this.registryCompressed = registryCompressed;
    }

    public String getControllerPath() {
        return controllerPath;
    }

    public void setControllerPath(String controllerPath) {
        this.controllerPath = controllerPath;
    }

    public EventBus<ContextEvent> getEventBus() {
        return eventBus;
    }

    public void setEventBus(EventBus<ContextEvent> eventBus) {
        this.eventBus = eventBus;
    }

    /**
     * 更新控制数据
     */
    protected void updateControllerInfo() {
        try {
            PathData pathData = registry.getData(controllerPath);
            updateControllerInfo(pathData.getData());
        } catch (RegistryException e) {
            logger.error(String.format("get data %s error.", controllerPath), e);
        }
    }

    /**
     * 更新控制数据
     *
     * @param data
     */
    protected void updateControllerInfo(byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }
        String text;
        if (registryCompressed) {
            try {
                text = new String(Compressors.decompress(data, Zip.INSTANCE));
            } catch (IOException e) {
                logger.error("decompress controller data error.", e);
                return;
            }
        } else {
            text = new String(data, Charsets.UTF_8);
        }
        if (logger.isInfoEnabled()) {
            logger.info("update controller.\n" + text);
        }
        List<AppPermission> controllerInfoList;
        try {
            controllerInfoList = JSON.parseArray(text, AppPermission.class);
        } catch (Exception e) {
            logger.error(String.format("parse controller data error."), e);
            return;
        }
        // 如果数据为空,清空所有内存数据
        if (controllerInfoList == null || controllerInfoList.isEmpty()) {
            eventBus.add(new ContextEvent(BrokerContext.CLEAR_ALL_CONTROLLER_DATA, null));
            controls.remove();
            return;
        }
        currentControllerUUIDs.clear();
        Set<String> tmpLastUUIDs = new HashSet<String>();
        for (AppPermission controllerInfo : controllerInfoList) {
            String topic = controllerInfo.getTopic();
            String app = controllerInfo.getApp();
            String group = controllerInfo.getGroup();
            String controllerType = controllerInfo.getAppPermissionType();
            String controllerContent = controllerInfo.getAppPermissionContent();
            String uuid = topic + SPECIAL_SPLIT_STRING + app + SPECIAL_SPLIT_STRING + group + SPECIAL_SPLIT_STRING + controllerType;
            String old;
            String combinationInfo = "";
            // 控制类别和控制信息不能为空
            if (StringUtils.isNotBlank(controllerType)) {
                if (StringUtils.isNotBlank(controllerContent)) {
                    combinationInfo += controllerContent;
                    combinationInfo += SPECIAL_SPLIT_STRING;
                    if (StringUtils.isNotBlank(group)) {
                        combinationInfo += group;
                    }
                    combinationInfo += SPECIAL_SPLIT_STRING;
                    combinationInfo += SPECIAL_SPLIT_STRING;
                    if (StringUtils.isNotBlank(topic)) {
                        combinationInfo += topic;
                    }
                    combinationInfo += SPECIAL_SPLIT_STRING;
                    if (StringUtils.isNotBlank(app)) {
                        combinationInfo += app;
                    }
                    currentControllerUUIDs.add(uuid);
                    if (combinationInfo != null) {
                        old = controls.getString(uuid, "");
                        if (!combinationInfo.equals(old)) {
                            controls.put(uuid, combinationInfo);
                            eventBus.add(new ContextEvent(controllerType, combinationInfo));
                        }
                    }
                } else {
                    continue;
                }
            } else {
                continue;
            }
        }
        tmpLastUUIDs.addAll(lastControllerUUIDs);
        tmpLastUUIDs.removeAll(currentControllerUUIDs);
        for (String deleteContrls : tmpLastUUIDs) {
            String controllerType = StringUtils.substringAfterLast(deleteContrls, SPECIAL_SPLIT_STRING);
            String topic = StringUtils.substringBefore(deleteContrls, SPECIAL_SPLIT_STRING);
            String app = StringUtils.substringBetween(deleteContrls, SPECIAL_SPLIT_STRING, SPECIAL_SPLIT_STRING);
            if (controllerType != null && controllerType.equals(BrokerContext.BROKER_APP_CONTROLLER_CLIENTIP)) {
                eventBus.add(new ContextEvent(BrokerContext.DELETE_OLD_APP_CONTROLLER_DATA, topic + SPECIAL_SPLIT_STRING + app));
            } else if (controllerType != null && controllerType.equals(BrokerContext.BROKER_TOPIC_CONTROLLER_PERMISSION)) {
                eventBus.add(new ContextEvent(BrokerContext.DELETE_OLD_TOPIC_CONTROLLER_DATA, topic));
            }
            controls.remove(deleteContrls);
        }
        lastControllerUUIDs.clear();
        lastControllerUUIDs.addAll(currentControllerUUIDs);
    }

    /**
     * 监听控制信息变化
     */
    protected class ControllerListener implements PathListener {
        @Override
        public void onEvent(PathEvent pathEvent) {
            writeLock.lock();
            try {
                if (!isStarted()) {
                    return;
                }
                updateControllerInfo(pathEvent.getData());
            } finally {
                writeLock.unlock();
            }
        }
    }
}
