package com.ipd.jmq.server.broker.handler.telnet.utils;

import com.ipd.jmq.common.network.v3.netty.telnet.param.MonitorParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangkepeng on 16-12-1.
 * 命令参数映射为方法名称,为了简化使用输入
 */
public class TelnetMap {

    private static final Logger logger = LoggerFactory.getLogger(TelnetMap.class);

    // 方法指令名称——》方法名
    public static Map<String, String> METHOD_MAP = new HashMap<String, String>();

    // 方法指令名称——》方法参数名
    public static Map<String, Object[]> PARAM_METHOD_MAP = new HashMap<String, Object[]>();

    static {
        {
            // 方法指令对应的方法名
            METHOD_MAP.put(MonitorParam.IdentityType.STAT.getIdentityType(), "getBrokerStat");
            METHOD_MAP.put(MonitorParam.IdentityType.PERF.getIdentityType(), "getPerformance");
            METHOD_MAP.put(MonitorParam.IdentityType.EXCE.getIdentityType(), "getBrokerExe");
            METHOD_MAP.put(MonitorParam.IdentityType.STATE.getIdentityType(), "getReplicationStates");
            METHOD_MAP.put(MonitorParam.IdentityType.REPS.getIdentityType(), "hasReplicas");
            METHOD_MAP.put(MonitorParam.IdentityType.ROLE.getIdentityType(), "getBrokerRole");
            METHOD_MAP.put(MonitorParam.IdentityType.VERSION.getIdentityType(), "getBrokerVersion");
            METHOD_MAP.put(MonitorParam.IdentityType.INFO.getIdentityType(), "restartAndStartTime");
            METHOD_MAP.put(MonitorParam.IdentityType.CONFIG.getIdentityType(), "getStoreConfig");
            METHOD_MAP.put(MonitorParam.IdentityType.SESSION.getIdentityType(), "getTopicSessionsOnBroker");
            METHOD_MAP.put(MonitorParam.IdentityType.CLIENT.getIdentityType(), "getConnections");
            METHOD_MAP.put(MonitorParam.IdentityType.OFFSETINFO.getIdentityType(), "getOffsetInfo");
            METHOD_MAP.put(MonitorParam.IdentityType.RESETOFFSET.getIdentityType(), "resetAckOffset");
            METHOD_MAP.put(MonitorParam.IdentityType.LOCATION.getIdentityType(), "getLocations");
            METHOD_MAP.put(MonitorParam.IdentityType.CLEANEXPIRE.getIdentityType(), "cleanExpire");
            METHOD_MAP.put(MonitorParam.IdentityType.NOACKBLOCKS.getIdentityType(), "getNoAckBlocks");
            METHOD_MAP.put(MonitorParam.IdentityType.CLOSEPRODUCER.getIdentityType(), "closeProducer");
            METHOD_MAP.put(MonitorParam.IdentityType.CLOSECONSUMER.getIdentityType(), "closeConsumer");
            METHOD_MAP.put(MonitorParam.IdentityType.VIEWMESSAGE.getIdentityType(), "viewMessage");
            METHOD_MAP.put(MonitorParam.IdentityType.UNSUBSCRIBE.getIdentityType(), "unsubscribe");
        }

        loadParamMethod();
    }

    // 加载命令中对应的参数域
    private static void loadParamMethod() {
        paramMethod(MonitorParam.IdentityType.SUBSCRIBE.getIdentityType(), "topic", "app");
        paramMethod(MonitorParam.IdentityType.CLIENT.getIdentityType(), "topic", "app");
        paramMethod(MonitorParam.IdentityType.LOCATION.getIdentityType(), "topic", "app");
        paramMethod(MonitorParam.IdentityType.CLEANEXPIRE.getIdentityType(), "topic", "app", "queueId");
        paramMethod(MonitorParam.IdentityType.OFFSETINFO.getIdentityType(), "topic", "app", "queueId");
        paramMethod(MonitorParam.IdentityType.CLOSEPRODUCER.getIdentityType(), "topic", "app");
        paramMethod(MonitorParam.IdentityType.CLOSECONSUMER.getIdentityType(), "topic", "app");
        paramMethod(MonitorParam.IdentityType.VIEWMESSAGE.getIdentityType(), "topic", "app","count");
        paramMethod(MonitorParam.IdentityType.RESETOFFSET.getIdentityType(), "topic", "queueId","app","value","bool");
        paramMethod(MonitorParam.IdentityType.UNSUBSCRIBE.getIdentityType(), "topic", "consumer");
    }

    private static void paramMethod(String invoke, Object... params) {

        if (params == null || params.length <= 0) {
            return;
        }
        Object[] objs = new Object[params.length];
        try {
            for (int index = 0; index < params.length; index++) {
                if (params[index] instanceof String) {
                    Field field = MonitorParam.class.getDeclaredField((String) params[index]);
                    field.setAccessible(true);
                    objs[index] = field;
                }
            }
            PARAM_METHOD_MAP.put(invoke, objs);
        } catch (NoSuchFieldException e) {
            logger.error("load app param method error, no such field", e);
        }
    }
}
