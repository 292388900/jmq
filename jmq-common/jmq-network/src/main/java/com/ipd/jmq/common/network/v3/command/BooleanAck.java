package com.ipd.jmq.common.network.v3.command;

/**
 * 布尔应答.
 *
 * @author lindeqiang
 * @since 2016/8/11 10:32
 */
public class BooleanAck extends JMQPayload {
    @Override
    public int type() {
        return CmdTypes.BOOLEAN_ACK;
    }
}
