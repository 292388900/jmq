package com.ipd.jmq.client.connection;

import com.ipd.jmq.common.network.ClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * 连接通道配置
 *
 * @author lindeqiang
 * @since 14-4-29 上午10:37
 */
public class TransportConfig extends ClientConfig {
    private static final Logger logger = LoggerFactory.getLogger(TransportConfig.class);
    // 默认的连接地址 地址:端口;地址:端口
    private String address = "jmq-cluster.ipd.local:80";
    // 用户名
    private String user;
    // 密码
    private String password;
    // 应用
    private String app;
    // 临时目录
    private String tempPath;

    public String getAddress() {
        return this.address;
    }

    public void setAddress(String address) {
        if (address != null) {
            address = address.trim();
            if (!address.isEmpty()) {
                this.address = address;
            }
        }
    }

    public String getUser() {
        return this.user;
    }

    public void setUser(String user) {
        if (user != null) {
            user = user.trim();
        }
        this.user = user;
    }

    public String getPassword() {
        return this.password;
    }

    public void setPassword(String password) {
        if (password != null) {
            password = password.trim();
        }
        this.password = password;
    }

    public String getApp() {
        return this.app;
    }

    public void setApp(String app) {
        if (app != null) {
            app = app.trim();
        }
        this.app = app;
    }

    public String getTempPath() {
        return tempPath;
    }

    public void setTempPath(String tempPath) {
        if (tempPath != null) {
            tempPath = tempPath.trim();
        }
        this.tempPath = tempPath;
    }

    /**
     * 获取初始化连接地址，会自动解析域名，过滤掉无效的地址
     *
     * @return 初始化连接地址列表
     */
    public List<String> getAddresses() {
        List<String> result = new ArrayList<String>();
        if (address != null) {
            String[] ips = address.split(";");
            String[] parts;
            int port;
            String addr;
            for (String ip : ips) {
                if (ip != null) {
                    ip = ip.trim();
                    if (!ip.isEmpty()) {
                        //解析域名
                        parts = ip.split(":");
                        if (parts.length == 2) {
                            try {
                                port = Integer.parseInt(parts[1]);
                                InetAddress[] inetAddresses = InetAddress.getAllByName(parts[0]);
                                for (InetAddress inetAddress : inetAddresses) {
                                    addr = inetAddress.getHostAddress() + ":" + port;
                                    if (logger.isDebugEnabled()) {
                                        logger.debug(String.format("resolve address success. %s", addr));
                                    }
                                    result.add(addr);
                                }
                            } catch (NumberFormatException ignored) {
                                logger.error(String.format("invalid port of config.address. %s", ip));
                            } catch (UnknownHostException ignored) {
                                logger.error(String.format("unknown host of config.address. %s", ip));
                            }
                        } else {
                            logger.error(String.format("invalid port of config.address. %s", ip));
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * 随机选择一个地址
     *
     * @return 地址
     */
    public InetSocketAddress getSocketAddress() {
        return getSocketAddress(getAddresses());
    }

    /**
     * 随机选择一个地址
     *
     * @param addresses 地址列表
     * @return 地址
     */
    public InetSocketAddress getSocketAddress(final List<String> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            return null;
        }
        int pos = (int) (Math.random() * addresses.size());
        return getSocketAddress(addresses.get(pos));
    }

    /**
     * 获取Socket地址
     *
     * @param address 地址
     * @return 地址
     */
    public InetSocketAddress getSocketAddress(final String address) {
        if (address == null || address.isEmpty()) {
            return null;
        }
        int pos = address.lastIndexOf(':');
        String ip = address.substring(0, pos);
        int port = Integer.parseInt(address.substring(pos + 1));
        return new InetSocketAddress(ip, port);
    }
}