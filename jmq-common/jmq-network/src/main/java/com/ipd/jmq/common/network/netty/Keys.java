package com.ipd.jmq.common.network.netty;

import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.protocol.telnet.TelnetInput;
import io.netty.util.AttributeKey;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by hexiaofeng on 16-7-7.
 */
public class Keys {
    // 全局唯一
    private static Map<String, AttributeKey> keys = new ConcurrentHashMap<String, AttributeKey>();

    // 协议属性Key
    public static final AttributeKey<Protocol> PROTOCOL = get(Protocol.PROTOCOL);
    public static final AttributeKey<Transport> TRANSPORT = get(Transport.TRANSPORT);
    public static final AttributeKey<TelnetInput> INPUT = get(TelnetInput.INPUT);

    /**
     * 获取属性Key
     *
     * @param key 键
     * @return 属性Key
     */
    public static AttributeKey get(final String key) {
        AttributeKey result = keys.get(key);
        if (result == null) {
            synchronized (keys) {
                result = keys.get(key);
                if (result == null) {
                    // 并发会报错
                    result = AttributeKey.valueOf(key);
                    keys.put(key, result);
                }
            }
        }
        return result;
    }

}
