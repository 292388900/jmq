package com.ipd.jmq.registry.provider;

import com.ipd.jmq.toolkit.URL;
import com.ipd.jmq.toolkit.network.http.Get;
import com.ipd.jmq.toolkit.lang.Charsets;
import com.ipd.jmq.toolkit.retry.Retry;
import com.ipd.jmq.toolkit.retry.RetryPolicy;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * HTTP远程请求注册中心
 *
 * @author hexiaofeng
 * @version 1.0.0
 * @since 12-12-12 下午5:03
 */
public class HttpProvider implements AddressProvider {

    public static final String SOCKET_TIMEOUT = "timeout";
    public static final String RETRY_TIMES = "retryTimes";
    public static final String CHARSET = "charset";
    public static final String CONNECTION_TIMEOUT = "connectionTimeout";
    public static final int DEFAULT_TIMEOUT = 5000;
    public static final int DEFAULT_CONNECTION_TIMEOUT = 1000;
    public static final int DEFAULT_RETRY_TIMES = 1;

    protected URL url;

    @Override
    public void setUrl(URL url) {
        this.url = url;
    }

    @Override
    public String getAddress() throws Exception {
        if (url == null) {
            throw new IllegalStateException("url is null");
        }
        if (url.getHost() == null) {
            throw new IllegalStateException("url is invalid");
        }
        int retryTimes = url.getPositive(RETRY_TIMES, DEFAULT_RETRY_TIMES);
        final int connectionTimeout = url.getPositive(CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
        final int socketTimeout = url.getPositive(SOCKET_TIMEOUT, DEFAULT_TIMEOUT);
        final String charset = url.getString(CHARSET, Charsets.UTF_8.name());
        final URL ul = url.remove(RETRY_TIMES, CONNECTION_TIMEOUT, SOCKET_TIMEOUT, CHARSET);

        String html = Retry.execute(new RetryPolicy(500, retryTimes), new Callable<String>() {
            @Override
            public String call() throws Exception {
                return html(ul, charset, connectionTimeout, socketTimeout);
            }
        });
        if (html == null || html.contains("<")) {
            return null;
        }

        return html;
    }

    /**
     * 获取html内容
     *
     * @param url               地址
     * @param charset           字符集
     * @param connectionTimeout 连接超时
     * @param socketTimeout     读取时间
     * @return
     * @throws IOException
     */
    protected String html(final URL url, final String charset, final int connectionTimeout,
                          final int socketTimeout) throws IOException {
        return Get.Builder.build().charset(charset).connectionTimeout(connectionTimeout).socketTimeout(socketTimeout)
                .create().get(url);
    }

    @Override
    public String getType() {
        return "http";
    }
}
