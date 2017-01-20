package com.ipd.jmq.server.broker.controller;

import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangkepeng on 16-5-20.
 */
public class ControllerPrintLog {

    private static final Logger logger = LoggerFactory.getLogger(ControllerPrintLog.class);

    protected static final long PRINT_LOG_TIME_INTERVAL = 1000 * 60 * 10;

    protected static final int APP_LEVEL_PRODUCE_PRINT_LOG = 1;

    protected static final int APP_LEVEL_CONSUME_PRINT_LOG = 2;

    protected static final int TOPIC_LEVEL_PRODUCE_PRINT_LOG = 3;

    protected static final int TOPIC_LEVEL_CONSUME_PRINT_LOG = 4;

    protected static final int APP_LIMIT_IP_PRODUCE = 5;

    protected static final int APP_LIMIT_IP_CONSUME = 6;

    protected static final int TOPIC_BROKER_LIMIT_WRITE = 7;

    protected static final int TOPIC_BROKER_LIMIT_READ = 8;

    protected static final String PRODUCER = "producer";

    protected static final String CONSUMER = "consumer";

    protected static final Map<String, Map<ControllerType, Long>> printTopicLogTimestampMap;

    protected static final Map<String, Map<ControllerType, Long>> printAppLogTimestampMap;

    static {
        // 控制日志输出,key: topic+PRODUCER或topic+CONSUMER, value记录上次打印日志时间,非线程安全
        printTopicLogTimestampMap = new HashMap<String, Map<ControllerType, Long>>();
        // 应用级别控制日志输出,key: app+PRODUCER或app+CONSUMER, value记录上次打印日志时间,非线程安全
        printAppLogTimestampMap = new HashMap<String, Map<ControllerType, Long>>();
    }

    /**
     * 确定是否需要打印日志,
     *
     * @param controllerType
     * @return
     */
    public static void printLog(String level, ControllerType controllerType, int type, int printType) {
        Long lastPrintLogTimestamp = 0L;
        String printLogKey = null;
        Map<ControllerType, Long> controllerTimestamp = null;
        if (APP_LEVEL_PRODUCE_PRINT_LOG == type) {
            printLogKey = level + PRODUCER;
            controllerTimestamp = printAppLogTimestampMap.get(printLogKey);
            if (controllerTimestamp == null || controllerTimestamp.isEmpty()) {
                controllerTimestamp = new EnumMap<ControllerType, Long>(ControllerType.class);
                controllerTimestamp.put(controllerType, SystemClock.getInstance().now());
                printAppLogTimestampMap.put(printLogKey, controllerTimestamp);
                printLogByPrintType(level, printType);
                return;
            }
        } else if (APP_LEVEL_CONSUME_PRINT_LOG == type) {
            printLogKey = level + CONSUMER;
            controllerTimestamp = printAppLogTimestampMap.get(printLogKey);
            if (controllerTimestamp == null || controllerTimestamp.isEmpty()) {
                controllerTimestamp = new EnumMap<ControllerType, Long>(ControllerType.class);
                controllerTimestamp.put(controllerType, SystemClock.getInstance().now());
                printAppLogTimestampMap.put(printLogKey, controllerTimestamp);
                printLogByPrintType(level, printType);
                return;
            }
        } else if (TOPIC_LEVEL_PRODUCE_PRINT_LOG == type) {
            printLogKey = level + PRODUCER;
            controllerTimestamp = printTopicLogTimestampMap.get(printLogKey);
            if (controllerTimestamp == null || controllerTimestamp.isEmpty()) {
                controllerTimestamp = new EnumMap<ControllerType, Long>(ControllerType.class);
                controllerTimestamp.put(controllerType, SystemClock.getInstance().now());
                printTopicLogTimestampMap.put(printLogKey, controllerTimestamp);
                printLogByPrintType(level, printType);
                return;
            }
        } else if (TOPIC_LEVEL_CONSUME_PRINT_LOG == type) {
            printLogKey = level + CONSUMER;
            controllerTimestamp = printTopicLogTimestampMap.get(printLogKey);
            if (controllerTimestamp == null || controllerTimestamp.isEmpty()) {
                controllerTimestamp = new EnumMap<ControllerType, Long>(ControllerType.class);
                controllerTimestamp.put(controllerType, SystemClock.getInstance().now());
                printTopicLogTimestampMap.put(printLogKey, controllerTimestamp);
                printLogByPrintType(level, printType);
                return;
            }
        }

        lastPrintLogTimestamp = controllerTimestamp.get(controllerType);
        if (lastPrintLogTimestamp == 0L) {
            // 该应用下没有相关打印日志时间戳
            printLogByPrintType(level, printType);
            controllerTimestamp.put(controllerType, SystemClock.getInstance().now());
            return;
        }
        if (SystemClock.now() - lastPrintLogTimestamp > PRINT_LOG_TIME_INTERVAL) {
            printLogByPrintType(level, printType);
            controllerTimestamp.put(controllerType, SystemClock.getInstance().now());
        }
    }

    /**
     * 更加类别和控制类型打印日志
     *
     */
    private static void printLogByPrintType(String level,int printType) {
        if (APP_LIMIT_IP_PRODUCE == printType) {
            logger.warn(level + " is limited by client setting to write the group");
        } else if (APP_LIMIT_IP_CONSUME == printType) {
            logger.warn(level + " is limited by client setting to read the group");
        } else if (TOPIC_BROKER_LIMIT_WRITE == printType) {
            logger.warn(level + " is limited to write the group");
        } else if (TOPIC_BROKER_LIMIT_READ == printType) {
            logger.warn(level + " is limited to read the group");
        }
    }
}
