package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.stat.ClientTPStat;
import com.ipd.jmq.common.stat.ClientTpOriginals;
import com.ipd.jmq.toolkit.lang.Preconditions;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 客户端性能
 */
public class ClientProfile extends JMQPayload {
    private List<ClientTPStat> clientStats;

    public ClientProfile clientStat(List<ClientTPStat> clientStat) {
        setClientStats(clientStat);
        return this;
    }

    public List<ClientTPStat> getClientStats() {
        return clientStats;
    }

    public void setClientStats(List<ClientTPStat> clientStats) {
        this.clientStats = clientStats;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(clientStats != null, "client stat can not be null.");
    }

    @Override
    public int type() {
        return CmdTypes.CLIENT_PROFILE;
    }

    /**
     * 性能
     *
     * @param out  输出缓冲区
     * @param stat 性能统计
     */
    public void write(final ByteBuf out, final ClientTpOriginals stat) throws Exception {
        out.writeLong(stat.getSuccess());
        Map<Integer, AtomicInteger> tpTimes = stat.getTpTimes();
        int count = 0;
        if (tpTimes != null) {
            count = tpTimes.size();
        }
        out.writeInt(count);
        if (count > 0) {
            for (Map.Entry<Integer, AtomicInteger> tpTime : tpTimes.entrySet()) {
                out.writeInt(tpTime.getKey());
                out.writeInt(tpTime.getValue() == null ? 0 : tpTime.getValue().get());
            }
        }
    }

    public int predictionSize() {
        int size = 4 + 1;
        // TODO: 11/24/16 size changed
        if (clientStats != null && !clientStats.isEmpty()) {
            size += Serializer.getPredictionSize(clientStats.get(0).getApp());
            for (ClientTPStat clientStat : clientStats) {
                size += Serializer.getPredictionSize(clientStat.getTopic());
                size += 15 * 8;
                size += Serializer.getPredictionSize(clientStat.getPingAvgResponseTime());
                size += 3 * (20 * 4 + 8);
            }
        }
        return size;
    }

    /**
     * 读取TP数据
     *
     * @param in   输入
     * @param stat 性能统计
     */
    public void read(final ByteBuf in, final ClientTpOriginals stat) throws Exception {
        stat.setSuccess(in.readLong());
        Map<Integer, AtomicInteger> tpTimes = new ConcurrentHashMap<>();
        int count = in.readInt();
        if (count > 0) {
            for (int i = 0; i < count; i++) {
                tpTimes.put(in.readInt(), new AtomicInteger(in.readInt()));
            }
        }

        stat.setTpTimes(tpTimes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientProfile{");
        sb.append("clientStats=").append(clientStats);
        sb.append('}');
        return sb.toString();
    }
}
