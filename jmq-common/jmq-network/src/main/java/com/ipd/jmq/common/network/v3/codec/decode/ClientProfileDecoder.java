package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.ClientProfile;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.stat.ClientTPStat;
import com.ipd.jmq.common.stat.ClientTpOriginals;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * 客户端性能解码器
 */
public class ClientProfileDecoder implements PayloadDecoder<ClientProfile> {

    @Override
    public ClientProfile decode(final ClientProfile payload, final ByteBuf in) throws Exception {
        List<ClientTPStat> clientStats = new ArrayList<ClientTPStat>();
        int size = in.readInt();
        if (size > 0) {
            String app = Serializer.readString(in, 1, false);
            ClientTPStat clientStat;
            // 读取基本属性
            for (int i = 0; i < size; i++) {
                clientStat = new ClientTPStat();
                clientStat.setTopic(Serializer.readString(in, 1, false));
                clientStat.setApp(app);
                clientStat.setStartTime(in.readLong());
                clientStat.setEndTime(in.readLong());
                clientStat.setProduce(new ClientTpOriginals(in.readLong(), 0, in.readLong(), in.readLong(), in.readLong()));
                clientStat.setReceive(new ClientTpOriginals(in.readLong(), 0, 0, in.readLong(), in.readLong()));
                clientStat.setConsume(new ClientTpOriginals(in.readLong(), 0, in.readLong(), 0, in.readLong()));
                clientStat.setRetry(new ClientTpOriginals(in.readLong(), 0, in.readLong(), 0, in.readLong()));

                clientStat.setCpuUsage(in.readDouble());
                clientStat.setMemUsage(in.readDouble());
                clientStat.setOneMinLoad(in.readDouble());
                clientStat.setPingAvgResponseTime(Serializer.readMap(in));

                clientStats.add(clientStat);
            }

            payload.setClientStats(clientStats);

            // 读取TP属性
            for (int i = 0; i < clientStats.size(); i++) {
                clientStat = clientStats.get(i);
                payload.read(in, (ClientTpOriginals) clientStat.getProduce());
                payload.read(in, (ClientTpOriginals) clientStat.getReceive());
                payload.read(in, (ClientTpOriginals) clientStat.getConsume());
                payload.read(in, (ClientTpOriginals) clientStat.getRetry());
            }
        }
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.CLIENT_PROFILE;
    }
}