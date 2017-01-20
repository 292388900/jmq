package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.TxPrepare;
import com.ipd.jmq.common.network.v3.session.ProducerId;
import com.ipd.jmq.common.network.v3.session.TransactionId;
import com.ipd.jmq.toolkit.time.SystemClock;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.Map;

/**
 * 分布式事务准备解码器
 */
public class TxPrepareDecoder implements PayloadDecoder<TxPrepare> {

    @Override
    public TxPrepare decode(final TxPrepare payload, final ByteBuf in) throws Exception {
        //n 字节的属性
        Map<Object, Object> attrs = payload.getAttrs();
        attrs.putAll(Serializer.readMap(in));
        //n 字节事务ID
        String txid = Serializer.readString(in);
        payload.setTransactionId(txid == null || txid.isEmpty() ? null : new TransactionId(txid));
        //n 字节生产者ID
        String pid = Serializer.readString(in);
        payload.setProducerId(pid == null || pid.isEmpty() ? null : new ProducerId(pid));
        //n 字节主题
        payload.setTopic(Serializer.readString(in));
        //n 字节查询ID
        payload.setQueryId(Serializer.readString(in));
        //设置事务开始时间
        long startTime = in.readLong();
        payload.setStartTime(startTime > 0 ? startTime : SystemClock.getInstance().now());
        //事务超时时间
        payload.setTimeout(in.readInt());
        //超时后默认操作
        payload.setTimeoutAction(in.readByte());
        //消息列表
        Message[] messageArray = Serializer.readMessages(in);
        if (messageArray != null && messageArray.length > 0) {
            payload.setMessages(Arrays.asList(messageArray));
        }

        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.TX_PREPARE;
    }
}