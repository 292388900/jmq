package com.ipd.jmq.common.network.v3.command;

/**
 * JMQ 命令构造器.
 *
 * @author lindeqiang
 * @since 2016/7/20 12:53
 */
public class DefaultCommandFactory implements CommandFactory {
    public Command create(Header header) {
        if (header == null) {
            return null;
        }
        try {
            Object payload = null;
            JMQHeader jmqHeader = (JMQHeader) header;
            Class clazz = CmdTypeClazz.typeClazz.get(jmqHeader.getType());
            if (clazz != null) {
                payload = clazz.newInstance();
            }
            return new Command(header, payload);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
