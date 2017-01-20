package com.ipd.jmq.common.network.v3.codec;

import com.ipd.jmq.common.network.netty.codec.Codec;
import com.ipd.jmq.common.network.v3.codec.decode.JMQDecoder;
import com.ipd.jmq.common.network.v3.codec.encode.JMQEncoder;
import com.ipd.jmq.common.network.v3.command.DefaultCommandFactory;
import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.common.network.v3.command.JMQPayload;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.command.Command;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMQ V3版本编解码器.
 *
 * @author lindeqiang
 * @since 2016/8/4 10:23
 */
public class JMQCodec implements Codec {
    public static final Logger logger = LoggerFactory.getLogger(JMQCodec.class);

    public static short MAGIC = 0x4d51; //MQ
    private JMQDecoder decoder;
    private JMQEncoder encoder;
    private DefaultCommandFactory commandFactory = new DefaultCommandFactory();

    public JMQCodec() {
        decoder = new JMQDecoder();
        encoder = new JMQEncoder();
    }

    @Override
    public Object decode(Class clazz, ByteBuf in) throws TransportException.CodecException {
        if (in == null) {
            return null;
        }
        try {
            //反序列化header
            JMQHeader header = decoder.decode(new JMQHeader(), in);
            //根据头部创建请求命令
            Command command = commandFactory.create(header);
            //反序列化payload
            JMQPayload payload = (JMQPayload) command.getPayload();
            if (payload != null) {
                command.setPayload(decoder.decode(payload, in));
            }
            if (logger.isDebugEnabled()) {
                logger.debug("decode command {}:{}, payload={}", header.getType(), header.getRequestId(), payload);
            }
            return command;
        } catch (Exception e) {
            throw new TransportException.CodecException(e.getMessage());
        }
    }

    @Override
    public void encode(Object obj, ByteBuf out) throws TransportException.CodecException {
        if (out == null || obj == null) {
            return;
        }

        if (obj instanceof Command) {
            //写magic
            out.writeShort(MAGIC);
            //命令长度，占位
            out.writeInt(0);
            //记录起始位置
            int begin = out.writerIndex();
            // 转换为command
            //类型转换
            Command command = (Command) obj;
            JMQHeader header = (JMQHeader) command.getHeader();
            JMQPayload payload = (JMQPayload) command.getPayload();

            if (logger.isDebugEnabled()) {
                logger.debug("encode command {}:{}, payload={}", header.getType(), header.getRequestId(), payload);
            }

            int capacity = begin + (header != null ? header.getPredictionSize() : 0) + (payload != null ? payload.predictionSize() : 0);
            if (out.capacity() < capacity) {
                out.capacity(capacity);
            }

            //header列化
            encoder.encode(header, out);
            //payload序列化

            if (payload != null) {
                encoder.encode(payload, out);
            }
            //设置消息长度
            int end = out.writerIndex();
            out.writerIndex(begin - 4);
            out.writeInt(end - begin);
            out.writerIndex(end);
        } else {
            throw new TransportException.CodecException(String.format("Command type %s not supported", obj.getClass()));
        }
    }
}
