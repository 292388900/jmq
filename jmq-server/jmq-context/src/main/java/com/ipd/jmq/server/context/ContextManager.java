package com.ipd.jmq.server.context;

import com.alibaba.fastjson.JSON;
import com.ipd.jmq.common.model.Config;
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
import com.ipd.jmq.toolkit.security.Base64;
import com.ipd.jmq.toolkit.security.Des;
import com.ipd.jmq.toolkit.security.Encrypt;
import com.ipd.jmq.toolkit.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.List;


/**
 * 上下文管理器
 */
public class ContextManager extends Service {
    private static final Logger logger = LoggerFactory.getLogger(ContextManager.class);
    // 系统配置
    private Context configs = new Context(null);
    // 注册中心
    private Registry registry;
    // 默认配置
    private ContextConfig config;
    // 系统配置监听器
    private PathListener configListener = new ConfigListener();
    // 事件派发器
    private EventBus<ContextEvent> eventManager = new EventBus<ContextEvent>("ContextManager");

    public ContextManager(ContextConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config can not be null");
        }
        this.config = config;
        this.registry = config.getRegistry();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        eventManager.start();
        if (registry.isConnected()) {
            // 初始化
            updateConfig();
        }

        registry.addListener(config.getConfigPath(), configListener);
        logger.info("context manager is started");
    }

    @Override
    protected void doStop() {
        super.doStop();
        registry.removeListener(config.getConfigPath(), configListener);
        eventManager.stop();
        configs.remove();
        logger.info("context manager is stopped");
    }

    /**
     * 添加监听器
     *
     * @param listener 监听器
     */
    public void addListener(EventListener<ContextEvent> listener) {
        if (eventManager.addListener(listener)) {
            if (isStarted()) {
                Iterator<Map.Entry<String, Object>> iterator = configs.iterator();
                Map.Entry<String, Object> entry;
                while (iterator.hasNext()) {
                    entry = iterator.next();
                    eventManager.add(new ContextEvent(entry.getKey(), (String) entry.getValue()), listener);
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
        eventManager.removeListener(listener);
    }

    /**
     * 获取参数
     *
     * @param name 参数名称
     * @return 参数对象
     */
    public String getParameter(final String name) {
        return configs.getString(name, null);
    }

    /**
     * 获取字符串参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 字符串参数
     */
    public String getParameter(final String name, final String defaultValue) {
        return configs.getString(name, defaultValue);
    }

    /**
     * 获取字节参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 浮点数参数
     */
    public byte getParameter(final String name, final byte defaultValue) {
        return configs.getByte(name, defaultValue);
    }

    /**
     * 获取短整数参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 浮点数参数
     */
    public short getParameter(final String name, final short defaultValue) {
        return configs.getShort(name, defaultValue);
    }

    /**
     * 获取整数参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 整数
     */
    public int getParameter(final String name, final int defaultValue) {
        return configs.getInteger(name, defaultValue);
    }

    /**
     * 获取长整形参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 长整形参数
     */
    public long getParameter(final String name, final long defaultValue) {
        return configs.getLong(name, defaultValue);
    }

    /**
     * 获取浮点数参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 浮点数参数
     */
    public double getParameter(final String name, final double defaultValue) {
        return configs.getDouble(name, defaultValue);
    }

    /**
     * 获取正整数参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 正整数
     */
    public byte getPositive(final String name, final byte defaultValue) {
        return configs.getByte(name, defaultValue);
    }

    /**
     * 获取正整数参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 正整数
     */
    public short getPositive(final String name, final short defaultValue) {
        return configs.getShort(name, defaultValue);
    }

    /**
     * 获取正整数参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 正整数
     */
    public int getPositive(final String name, final int defaultValue) {
        return configs.getPositive(name, defaultValue);
    }

    /**
     * 获取长正整数参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 长正整数
     */
    public long getPositive(final String name, final long defaultValue) {
        return configs.getPositive(name, defaultValue);
    }

    /**
     * 获取自然数参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 自然数
     */
    public byte getNatural(final String name, final byte defaultValue) {
        return configs.getNatural(name, defaultValue);
    }

    /**
     * 获取自然数参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 自然数
     */
    public short getNatural(final String name, final short defaultValue) {
        return configs.getNatural(name, defaultValue);
    }

    /**
     * 获取自然数参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 自然数
     */
    public int getNatural(final String name, final int defaultValue) {
        return configs.getNatural(name, defaultValue);
    }

    /**
     * 获取自然数参数
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 自然数
     */
    public long getNatural(final String name, final long defaultValue) {
        return configs.getNatural(name, defaultValue);
    }

    /**
     * 获取布尔值
     *
     * @param name         名称
     * @param defaultValue 默认值
     * @return 布尔值
     */
    public boolean getParameter(final String name, final boolean defaultValue) {
        return configs.getBoolean(name, defaultValue);
    }

    /**
     * 更新配置数据
     */
    protected void updateConfig() {
        try {
            PathData pathData = registry.getData(config.getConfigPath());
            updateConfig(pathData.getData());
        } catch (RegistryException e) {
            logger.error(String.format("get data %s error.", config.getConfigPath()), e);
        }
    }

    /**
     * 更新配置数据
     *
     * @param data 数据
     */
    protected void updateConfig(byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }
        String text;
        if (config.isCompressed()) {
            try {
                text = Compressors.decompress(data, Charsets.UTF_8, Zip.INSTANCE);
            } catch (IOException e) {
                logger.error("decompress config data error.", e);
                return;
            }
        } else {
            text = new String(data, Charsets.UTF_8);
        }
        if (logger.isInfoEnabled()) {
            logger.info("update config.\n" + text);
        }
        List<Config> infoList;
        try {
            infoList = JSON.parseArray(text, Config.class);
        } catch (Exception e) {
            logger.error(String.format("parse config data error."), e);
            return;
        }
        if (infoList != null && !infoList.isEmpty()) {
            String value;
            String key;
            String old;
            for (Config info : infoList) {
                key = info.getCode();
                value = info.getValue();
                if (info.isPassword()) {
                    try {
                        // 解密密码
                        data = Base64.decode(value);
                        data = Des.INSTANCE.decrypt(data, Encrypt.DEFAULT_KEY.getBytes());
                        value = new String(data, Charsets.UTF_8);
                    } catch (Exception e) {
                        logger.error(String.format("decrypt config %s error. ", key), e);
                        // 出错忽略
                        continue;
                    }
                }
                if (value != null && key!=null) {
                    old = configs.getString(key, "");
                    if (!value.equals(old)) {
                        configs.put(key, value);
                        eventManager.add(new ContextEvent(key, value));
                    }
                }
            }
        }
    }


    /**
     * 监听配置数据变化
     */
    protected class ConfigListener implements PathListener {
        @Override
        public void onEvent(PathEvent pathEvent) {
            getWriteLock().lock();
            try {
                if (!isStarted()) {
                    return;
                }
                updateConfig(pathEvent.getData());
            } finally {
                getWriteLock().unlock();
            }
        }
    }

}
