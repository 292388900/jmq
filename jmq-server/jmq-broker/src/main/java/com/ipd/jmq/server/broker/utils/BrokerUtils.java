package com.ipd.jmq.server.broker.utils;

import com.alibaba.fastjson.JSON;
import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.network.v3.session.Connection;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.toolkit.lang.Pair;
import com.ipd.jmq.toolkit.network.Ipv4;

import java.io.*;
import java.net.InetSocketAddress;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lindeqiang
 * @since 2016/6/2 8:22
 */
public class BrokerUtils {

    public static void setBrokerMessage(final BrokerMessage message, final Pair<byte[], byte[]> cs, final Connection
            connection, final Transport transport, final Broker broker, long now) {
        if (message == null) {
            return;
        }

        if (cs == null || (connection == null && (transport == null || broker == null))) {
            throw new IllegalArgumentException("Arguments can not be null");
        }

        // 获取客户端地址
        if (cs.getKey() == null) {
            if (connection != null) {
                cs.setKey(connection.getAddress());
            } else {
                cs.setKey(Ipv4.toByte((InetSocketAddress) transport.remoteAddress()));
            }
        }

        //获取服务端地址
        if (cs.getValue() == null) {
            if (connection != null) {
                cs.setValue(connection.getServerAddress());
            } else {
                cs.setValue(Ipv4.toByte(broker.getName()));
            }
        }

        message.setReceiveTime(now);
        message.setClientAddress(cs.getKey());
        message.setServerAddress(cs.getValue());
    }

    // 比较版本号
    public static int compareVersion(String versionOne, String versionTwo) {
        String[] clientVersionArray = versionOne.split("\\.");
        String[] needMinVersionArray = versionTwo.split("\\.");
        int result = 0;
        int index = 0;
        int minLength = Math.min(clientVersionArray.length, needMinVersionArray.length);//取最小长度值
        while (index < minLength
                && (result = clientVersionArray[index].length() - needMinVersionArray[index].length()) == 0//先比较长度
                && (result = clientVersionArray[index].compareTo(needMinVersionArray[index])) == 0) {//再比较字符
            ++index;
        }
        //相等的情况下,有子版本的为大；
        result = (result != 0) ? result : clientVersionArray.length - needMinVersionArray.length;
        return result;
    }

    public static Object readConfigFile(File file, Class objClass, Object defValue) throws IOException {
        if (file == null || !file.exists()) {
            if (defValue == null)
                throw new IOException("file is null or not exists");
            else
                return defValue;
        }

        FileInputStream reader = new FileInputStream(file);
        byte[] buf = null;
        try {
            if (reader.available() > 1 * 1024 * 1024) {
                throw new IOException("file " + file.getAbsolutePath() + " is too large to process");
            } else if (reader.available() == 0) {
                return defValue;
            }
            buf = new byte[reader.available()];
            reader.read(buf);
        } finally {
            reader.close();
        }

        if (objClass.equals(String.class)) {
            return new String(buf);
        } else {
            Object retObj = JSON.parseObject(buf, objClass);
            return (retObj == null ? defValue : retObj);
        }
    }

    public static void writeConfigFile(File file, Object content) throws IOException {
        BufferedWriter writer = null;
        try {
            if (file != null) {
                if (!file.exists()) {
                    file.getParentFile().mkdirs();
                    file.createNewFile();
                }
                writer = new BufferedWriter(new FileWriter(file));
                if (content instanceof String) {
                    writer.write((String) content);
                } else {
                    writer.write(JSON.toJSONString(content));
                }
                writer.flush();
            }
        } finally {
            if (writer != null) writer.close();
        }
    }
}
