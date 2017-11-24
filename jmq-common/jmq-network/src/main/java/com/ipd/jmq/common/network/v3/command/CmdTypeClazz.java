package com.ipd.jmq.common.network.v3.command;

import java.util.HashMap;
import java.util.Map;

/**
 * 命令类型与class映射关系.
 *
 * @author lindeqiang
 * @since 2016/8/11 9:15
 */
public class CmdTypeClazz {
    // 命令类型和class映射关系
    public static final Map<Integer, Class> typeClazz = new HashMap<Integer, Class>();
    // class 与命令类型映射关系
    public static final Map<Class, Integer> clazzType = new HashMap<Class, Integer>();

    static {
        typeClazz.put(CmdTypes.GET_MESSAGE, GetMessage.class);
        typeClazz.put(CmdTypes.GET_MESSAGE_ACK, GetMessageAck.class);
        typeClazz.put(CmdTypes.ACK_MESSAGE, AckMessage.class);
        typeClazz.put(CmdTypes.RETRY_MESSAGE, RetryMessage.class);
        typeClazz.put(CmdTypes.PUT_RETRY, PutRetry.class);
        typeClazz.put(CmdTypes.GET_RETRY, GetRetry.class);
        typeClazz.put(CmdTypes.GET_RETRY_ACK, GetRetryAck.class);
        typeClazz.put(CmdTypes.UPDATE_RETRY, UpdateRetry.class);
        typeClazz.put(CmdTypes.GET_RETRY_COUNT, GetRetryCount.class);
        typeClazz.put(CmdTypes.GET_RETRY_ACK, GetRetryCountAck.class);
        typeClazz.put(CmdTypes.TX_PREPARE, TxPrepare.class);
        typeClazz.put(CmdTypes.TX_PREPARE_ACK, TxPrepareAck.class);
        typeClazz.put(CmdTypes.TX_COMMIT, TxCommit.class);
        typeClazz.put(CmdTypes.TX_ROLLBACK, TxRollback.class);
        typeClazz.put(CmdTypes.TX_FEEDBACK, TxFeedback.class);
        typeClazz.put(CmdTypes.TX_FEEDBACK_ACK, TxFeedbackAck.class);
        typeClazz.put(CmdTypes.PUT_MESSAGE_RICH, PutRichMessage.class);
        typeClazz.put(CmdTypes.PUT_MESSAGE, PutMessage.class);
        typeClazz.put(CmdTypes.HEARTBEAT, Heartbeat.class);
        typeClazz.put(CmdTypes.GET_CLUSTER, GetCluster.class);
        typeClazz.put(CmdTypes.GET_CLUSTER_ACK, GetClusterAck.class);
        typeClazz.put(CmdTypes.GET_PRODUCER_HEALTH, GetProducerHealth.class);
        typeClazz.put(CmdTypes.GET_CONSUMER_HEALTH, GetConsumerHealth.class);
        typeClazz.put(CmdTypes.ADD_CONNECTION, AddConnection.class);
        typeClazz.put(CmdTypes.REMOVE_CONNECTION, RemoveConnection.class);
        typeClazz.put(CmdTypes.ADD_PRODUCER, AddProducer.class);
        typeClazz.put(CmdTypes.REMOVE_PRODUCER, RemoveProducer.class);
        typeClazz.put(CmdTypes.ADD_CONSUMER, AddConsumer.class);
        typeClazz.put(CmdTypes.REMOVE_CONSUMER, RemoveConsumer.class);
        typeClazz.put(CmdTypes.CLIENT_PROFILE, ClientProfile.class);
        typeClazz.put(CmdTypes.CLIENT_PROFILE_ACK, ClientProfileAck.class);
        typeClazz.put(CmdTypes.IDENTITY, Identity.class);
        typeClazz.put(CmdTypes.GET_OFFSET, GetOffset.class);
        typeClazz.put(CmdTypes.GET_OFFSET_ACK, GetOffsetAck.class);
        typeClazz.put(CmdTypes.GET_JOURNAL, GetJournal.class);
        typeClazz.put(CmdTypes.GET_JOURNAL_ACK, GetJournalAck.class);

        typeClazz.put(CmdTypes.GET_CONSUMER_OFFSET, GetConsumeOffset.class);
        typeClazz.put(CmdTypes.GET_CONSUMER_OFFSET_ACK, GetConsumeOffsetAck.class);
        typeClazz.put(CmdTypes.SYSTEM_COMMAND, SystemCmd.class);
        typeClazz.put(CmdTypes.RESET_ACK_OFFSET, ResetAckOffset.class);


        for (Map.Entry<Integer, Class> entry : typeClazz.entrySet()) {
            clazzType.put(entry.getValue(), entry.getKey());
        }
    }
}
