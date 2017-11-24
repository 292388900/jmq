package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.ClientProfile;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.stat.ClientTPStat;
import com.ipd.jmq.common.stat.ClientTpOriginals;
import io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * 客户端性能编码器
 */
public class ClientProfileEncoder implements PayloadEncoder<ClientProfile> {

    @Override
    public void encode(final ClientProfile payload, final ByteBuf out) throws Exception {
        payload.validate();
        //1字节版本号
        List<ClientTPStat> clientStats = payload.getClientStats();
        if (clientStats != null && !clientStats.isEmpty()) {
            out.writeInt(clientStats.size());
            // APP
            Serializer.write(clientStats.get(0).getApp(), out, 1, false);
            ClientTpOriginals produce;
            ClientTpOriginals receive;
            ClientTpOriginals consume;
            ClientTpOriginals retry;
            // 遍历性能统计
            for (ClientTPStat clientStat : clientStats) {
                produce = (ClientTpOriginals) clientStat.getProduce();
                receive = (ClientTpOriginals) clientStat.getReceive();
                consume = (ClientTpOriginals) clientStat.getConsume();
                retry = (ClientTpOriginals) clientStat.getRetry();
                // 主题
                Serializer.write(clientStat.getTopic(), out, 1, false);
                // 时间
                out.writeLong(clientStat.getStartTime());
                out.writeLong(clientStat.getEndTime());
                // 生产统计
                out.writeLong(produce.getCount());
                out.writeLong(produce.getError());
                out.writeLong(produce.getSize());
                out.writeLong(produce.getTime());
                // 接收统计
                out.writeLong(receive.getCount());
                out.writeLong(receive.getSize());
                out.writeLong(receive.getTime());
                // 消费统计
                out.writeLong(consume.getCount());
                out.writeLong(consume.getError());
                out.writeLong(consume.getTime());
                //消费重试统计
                out.writeLong(retry.getCount());
                out.writeLong(retry.getError());
                out.writeLong(retry.getTime());

                // machine metrics
                out.writeDouble(clientStat.getCpuUsage());
                out.writeDouble(clientStat.getMemUsage());
                out.writeDouble(clientStat.getOneMinLoad());
                Serializer.write(clientStat.getPingAvgResponseTime(), out);

            }

            // 追加TP性能数据在后面，保持和原有数据协议的兼容
            for (ClientTPStat clientStat : clientStats) {
                // 生产TP
                payload.write(out, (ClientTpOriginals) clientStat.getProduce());
                // 接收TP
                payload.write(out, (ClientTpOriginals) clientStat.getReceive());
                // 消费TP
                payload.write(out, (ClientTpOriginals) clientStat.getConsume());
                //重试TP
                payload.write(out, (ClientTpOriginals) clientStat.getRetry());
            }
        } else {
            out.writeInt(0);
        }
    }

    @Override
    public int type() {
        return CmdTypes.CLIENT_PROFILE;
    }
}