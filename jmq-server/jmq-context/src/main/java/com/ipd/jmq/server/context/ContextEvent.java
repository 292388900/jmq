package com.ipd.jmq.server.context;

/**
 * 上下文变量变更事件
 */
public class ContextEvent {

    public static final String BROKER_GROUP = "broker";
    public static final String HA_GROUP = "ha";
    public static final String RETRY_GROUP = "retry";
    public static final String RETRY_SERVER_GROUP = "rs";
    public static final String NETTY_SERVER_GROUP = "server";
    public static final String NETTY_CLIENT_GROUP = "client";
    public static final String STORE_GROUP = "store";
    public static final String ARCHIVE_GROUP = "archive";
    public static final String CONTROL_GROUP = "control";




    // 键
    private String key;
    // 值
    private String value;

    public ContextEvent(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    /**
     * 获取短整数参数
     *
     * @param defaultValue 默认值
     * @return 短整数
     */
    public short getShort(final short defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Short.parseShort(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 获取短整数参数
     *
     * @param defaultValue 默认值
     * @return 短整数
     */
    public byte getByte(final byte defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Byte.parseByte(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 获取整数参数
     *
     * @param defaultValue 默认值
     * @return 整数
     */
    public int getInt(final int defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 获取浮点数
     *
     * @param defaultValue 默认值
     * @return 整数
     */
    public double getDouble(final double defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }


    /**
     * 获取长整形参数
     *
     * @param defaultValue 默认值
     * @return 长整形参数
     */
    public long getLong(final long defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 获取字节正整数参数
     *
     * @param defaultValue 默认值
     * @return 字节正整数
     */
    public byte getPositive(final byte defaultValue) {
        if (defaultValue <= 0) {
            throw new IllegalArgumentException("defaultValue <= 0");
        }
        byte result = getByte(defaultValue);
        return result <= 0 ? defaultValue : result;
    }

    /**
     * 获取短正整数参数
     *
     * @param defaultValue 默认值
     * @return 短正整数
     */
    public short getPositive(final short defaultValue) {
        if (defaultValue <= 0) {
            throw new IllegalArgumentException("defaultValue <= 0");
        }
        short result = getShort(defaultValue);
        return result <= 0 ? defaultValue : result;
    }

    /**
     * 获取正整数参数
     *
     * @param defaultValue 默认值
     * @return 正整数
     */
    public int getPositive(final int defaultValue) {
        if (defaultValue <= 0) {
            throw new IllegalArgumentException("defaultValue <= 0");
        }
        int result = getInt(defaultValue);
        return result <= 0 ? defaultValue : result;
    }


    /**
     * 获取长正整数参数
     *
     * @param defaultValue 默认值
     * @return 长正整数
     */
    public long getPositive(final long defaultValue) {
        if (defaultValue <= 0) {
            throw new IllegalArgumentException("defaultValue <= 0");
        }
        long result = getLong(defaultValue);
        return result <= 0 ? defaultValue : result;
    }

    /**
     * 获取自然数参数
     *
     * @param defaultValue 默认值
     * @return 自然数
     */
    public byte getNatural(final byte defaultValue) {
        if (defaultValue < 0) {
            throw new IllegalArgumentException("defaultValue < 0");
        }
        byte result = getByte(defaultValue);
        return result < 0 ? defaultValue : result;
    }

    /**
     * 获取自然数参数
     *
     * @param defaultValue 默认值
     * @return 自然数
     */
    public short getNatural(final short defaultValue) {
        if (defaultValue < 0) {
            throw new IllegalArgumentException("defaultValue < 0");
        }
        short result = getShort(defaultValue);
        return result < 0 ? defaultValue : result;
    }

    /**
     * 获取自然数参数
     *
     * @param defaultValue 默认值
     * @return 自然数
     */
    public int getNatural(final int defaultValue) {
        if (defaultValue < 0) {
            throw new IllegalArgumentException("defaultValue < 0");
        }
        int result = getInt(defaultValue);
        return result < 0 ? defaultValue : result;
    }

    /**
     * 获取自然数参数
     *
     * @param defaultValue 默认值
     * @return 自然数
     */
    public long getNatural(final long defaultValue) {
        if (defaultValue < 0) {
            throw new IllegalArgumentException("defaultValue < 0");
        }
        long result = getLong(defaultValue);
        return result < 0 ? defaultValue : result;
    }

    /**
     * 获取布尔值
     *
     * @param defaultValue 默认值
     * @return 布尔值
     */
    public boolean getBoolean(final boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        try {
            return Long.parseLong(value) != 0;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

}