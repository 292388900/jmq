package com.ipd.jmq.common.network.kafka.exception;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangkepeng on 16-8-4.
 */
public final class ErrorCode {

    public static Map<Class<Throwable>, Short> kafkaExcToCodeMap = new HashMap<Class<Throwable>, Short>();
    public static Map<Integer, Short> jmqExcToKafkaExc = new HashMap<Integer, Short>();

    // kafka异常类
    public static final Class INVALID_MESSAGE_EXCEPTION = InvalidMessageException.class;
    public static final Class INVALID_MESSAGE_SIZE_EXCEPTION = InvalidMessageSizeException.class;
    public static final Class LEADER_NOT_AVAILABLE_EXCEPTION = LeaderNotAvailableException.class;
    public static final Class MESSAGE_SET_SIZE_TOO_LARGE_EXCEPTION = MessageSetSizeTooLargeException.class;
    public static final Class MESSAGE_SIZE_TOO_LARGE_EXCEPTION = MessageSizeTooLargeException.class;
    public static final Class CONTROLLER_MOVED_EXCEPTION = ControllerMovedException.class;

    // JMQ错误码
    public static final int SUCCESS = 0;
    public static final int CN_NO_PERMISSION = 1;
    public static final int CN_AUTHENTICATION_ERROR = 2;
    public static final int CN_SERVICE_NOT_AVAILABLE = 3;
    public static final int CN_UNKNOWN_ERROR = 4;
    public static final int CN_DB_ERROR = 5;
    public static final int CN_PARAM_ERROR = 6;
    public static final int CN_NEGATIVE_VOTE = 7;
    public static final int CN_CHECKSUM_ERROR = 8;

    public static final int CN_CONNECTION_ERROR = 20;
    public static final int CN_CONNECTION_TIMEOUT = 21;
    public static final int CN_REQUEST_TIMEOUT = 22;
    public static final int CN_REQUEST_ERROR = 23;
    public static final int CN_REQUEST_EXCESSIVE = 24;
    public static final int CN_THREAD_INTERRUPTED = 25;
    public static final int CN_THREAD_EXECUTOR_BUSY = 26;
    public static final int CN_COMMAND_UNSUPPORTED = 27;
    public static final int CN_DECODE_ERROR = 28;
    public static final int CN_PLUGIN_NOT_IMPLEMENT = 29;

    public static final int CN_TRANSACTION_PREPARE_ERROR = 30;
    public static final int CN_TRANSACTION_EXECUTE_ERROR = 31;
    public static final int CN_TRANSACTION_COMMIT_ERROR = 32;
    public static final int CN_TRANSACTION_ROLLBACK_ERROR = 33;
    public static final int CN_TRANSACTION_NOT_EXISTS = 34;
    public static final int CN_TRANSACTION_UNSUPPORTED = 35;

    public static final int CT_NO_CLUSTER = 71;
    public static final int CT_SEQUENTIAL_BROKER_AMBIGUOUS = 72;
    public static final int CT_NO_CONSUMER_RECORD = 73;
    public static final int CT_LIMIT_REQUEST = 74;
    public static final int CT_LOW_VERSION = 75;
    public static final int CT_MESSAGE_BODY_NULL = 76;

    public static final int SE_IO_ERROR = 91;
    public static final int SE_OFFSET_OVERFLOW = 92;
    public static final int SE_MESSAGE_SIZE_EXCEEDED = 93;
    public static final int SE_DISK_FULL = 94;
    public static final int SE_CREATE_FILE_ERROR = 95;
    public static final int SE_FLUSH_TIMEOUT = 96;
    public static final int SE_INVALID_JOURNAL = 97;
    public static final int SE_INVALID_OFFSET = 98;
    public static final int SE_REPLICATION_ERROR = 99;
    public static final int SE_ENQUEUE_SLOW = 100;
    public static final int SE_DISK_FLUSH_SLOW = 101;
    public static final int SE_APPEND_MESSAGE_SLOW = 102;
    public static final int SE_QUEUE_NOT_EXISTS = 103;
    public static final int SE_FATAL_ERROR = 104;

    public static final int CY_REPLICATE_ENQUEUE_TIMEOUT = 111;
    public static final int CY_REPLICATE_TIMEOUT = 112;
    public static final int CY_REPLICATE_ERROR = 113;
    public static final int CY_GET_OFFSET_ERROR = 114;
    public static final int CY_FLUSH_OFFSET_ERROR = 115;
    public static final int CY_STATUS_ERROR = 116;
    public static final int CY_NOT_DEGRADE = 117;

    public static final int FW_CONNECTION_EXISTS = 131;
    public static final int FW_CONNECTION_NOT_EXISTS = 132;
    public static final int FW_PRODUCER_NOT_EXISTS = 134;
    public static final int FW_CONSUMER_NOT_EXISTS = 136;
    public static final int FW_TRANSACTION_EXISTS = 137;
    public static final int FW_TRANSACTION_NOT_EXISTS = 138;
    public static final int FW_COMMIT_ERROR = 139;
    public static final int FW_CONSUMER_ACK_FAIL = 140;
    public static final int FW_PUT_MESSAGE_ERROR = 141;
    public static final int FW_GET_MESSAGE_ERROR = 142;
    public static final int FW_FLUSH_SEQUENTIAL_STATE_ERROR = 143;
    public static final int FW_PUT_MESSAGE_TOPIC_NOT_WRITE = 144;
    public static final int FW_GET_MESSAGE_TOPIC_NOT_READ = 145;
    public static final int FW_PUT_MESSAGE_APP_CLIENT_IP_NOT_WRITE = 146;
    public static final int FW_GET_MESSAGE_APP_CLIENT_IP_NOT_READ = 147;
    public static final int FW_TRANSACTION_LIMIT = 148;

    public static final int JA_COMMAND_ERROR = 151;
    public static final int JA_COMMAND_EXISTS = 152;

    public static final int TN_COMMAND_NOT_EXISTS = 161;
    public static final int TN_COMMAND_ERROR = 162;
    public static final int TN_COMMAND_FORMAT_ERROR = 163;
    public static final int TN_COMMAND_PWD_ERROR = 164;

    // Kafka定义的错误编码
    public static final short UNKNOWN = -1;
    public static final short NO_ERROR = 0;
    public static final short OFFSET_OUTOFRANGE = 1;
    public static final short INVALID_MESSAGE = 2;
    public static final short UNKNOWN_TOPIC_OR_PARTITION = 3;
    public static final short INVALID_FETCH_SIZE = 4;
    public static final short LEADER_NOT_AVAILABLE = 5;
    public static final short NOTLEADER_FOR_PARTITION = 6;
    public static final short REQUEST_TIMEOUT = 7;
    public static final short BROKER_NOTAVAILABEL = 8;
    public static final short REPLICA_NOTAVAILABLE = 9;
    public static final short MESSAGE_SIZE_TOOLARGE = 10;
    public static final short STALE_CONTROLLER_EPOCH = 11;
    public static final short OFFSET_METADATA_TOO_LARGE = 12;
    public static final short STALELEADER_EPOCH = 13;
    public static final short OFFSETS_LOAD_INPROGRESS = 14;
    public static final short GROUP_COORDINATOR_NOT_AVAILABLE = 15;
    public static final short NOT_COORDINATOR_FOR_CONSUMER = 16;
    public static final short INVALID_TOPIC = 17;
    public static final short MESSAGESET_SIZE_TOO_LARGE = 18;
    public static final short NOTENOUGH_REPLICAS = 19;
    public static final short NOTENOUGH_REPLICAS_AFTER_APPEND = 20;
    public static final short ILLEGAL_GENERATION = 22;
    public static final short INCONSISTENT_GROUP_PROTOCOL = 23;
    public static final short INVALID_GROUP_ID = 24;
    public static final short UNKNOWN_MEMBER_ID = 25;
    public static final short INVALID_SESSION_TIMEOUT = 26;
    public static final short REBALANCE_IN_PROGRESS = 27;
    public static final short TOPIC_AUTHORIZATION_FAILED = 29;

    static {
        // Kafka错误映射
        kafkaExcToCodeMap.put(INVALID_MESSAGE_EXCEPTION, INVALID_MESSAGE);
        kafkaExcToCodeMap.put(INVALID_MESSAGE_SIZE_EXCEPTION, INVALID_FETCH_SIZE);
        kafkaExcToCodeMap.put(LEADER_NOT_AVAILABLE_EXCEPTION, LEADER_NOT_AVAILABLE);
        kafkaExcToCodeMap.put(MESSAGE_SET_SIZE_TOO_LARGE_EXCEPTION, MESSAGESET_SIZE_TOO_LARGE);
        kafkaExcToCodeMap.put(MESSAGE_SIZE_TOO_LARGE_EXCEPTION, MESSAGE_SIZE_TOOLARGE);
        kafkaExcToCodeMap.put(CONTROLLER_MOVED_EXCEPTION, STALE_CONTROLLER_EPOCH);
        // JMQ错误映射
        jmqExcToKafkaExc.put(SUCCESS, NO_ERROR);
        jmqExcToKafkaExc.put(CN_NO_PERMISSION, UNKNOWN_TOPIC_OR_PARTITION);
        jmqExcToKafkaExc.put(CN_AUTHENTICATION_ERROR, UNKNOWN_TOPIC_OR_PARTITION);
        jmqExcToKafkaExc.put(CN_SERVICE_NOT_AVAILABLE, LEADER_NOT_AVAILABLE);
        jmqExcToKafkaExc.put(CN_CHECKSUM_ERROR, INVALID_MESSAGE);
        jmqExcToKafkaExc.put(CN_CONNECTION_ERROR, BROKER_NOTAVAILABEL);
        jmqExcToKafkaExc.put(CN_CONNECTION_TIMEOUT, BROKER_NOTAVAILABEL);
        jmqExcToKafkaExc.put(CN_REQUEST_TIMEOUT, REQUEST_TIMEOUT);
        jmqExcToKafkaExc.put(CN_REQUEST_ERROR, REQUEST_TIMEOUT);
        jmqExcToKafkaExc.put(CN_REQUEST_EXCESSIVE, REQUEST_TIMEOUT);
        jmqExcToKafkaExc.put(CN_THREAD_INTERRUPTED, REQUEST_TIMEOUT);
        jmqExcToKafkaExc.put(CN_THREAD_EXECUTOR_BUSY, REQUEST_TIMEOUT);
        jmqExcToKafkaExc.put(CT_NO_CLUSTER, NOTLEADER_FOR_PARTITION);
        jmqExcToKafkaExc.put(CT_MESSAGE_BODY_NULL, INVALID_MESSAGE);
        jmqExcToKafkaExc.put(SE_OFFSET_OVERFLOW, OFFSET_OUTOFRANGE);
        jmqExcToKafkaExc.put(SE_MESSAGE_SIZE_EXCEEDED, MESSAGESET_SIZE_TOO_LARGE);
        jmqExcToKafkaExc.put(CY_REPLICATE_TIMEOUT, REPLICA_NOTAVAILABLE);
    }

    public static short codeFor(Class<? extends Throwable> exception) {
        Short code = kafkaExcToCodeMap.get(exception);
        if (code == null) {
            code = UNKNOWN;
        }
        return code;
    }

    public static short codeFor(int jmqCode) {
        Short code = jmqExcToKafkaExc.get(jmqCode);
        if (code == null) {
            code = UNKNOWN;
        }
        return code;
    }
}
