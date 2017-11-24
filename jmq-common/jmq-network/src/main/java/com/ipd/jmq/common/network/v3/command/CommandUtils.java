package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.model.Acknowledge;

/**
 * 命令工具类.
 *
 * @author lindeqiang
 * @since 2016/7/20 15:20
 */
public class CommandUtils {
    /**
     * 构造布尔应答
     *
     * @param requestId 请求ID
     * @param code      代码
     * @return 布尔应答
     */
    public static Command createBooleanAck(int requestId, JMQCode code) {
        JMQHeader.Builder builder = JMQHeader.Builder.create().direction(Direction.RESPONSE).type(CmdTypes
                .BOOLEAN_ACK).requestId(requestId).status(code.getCode()).acknowledge(Acknowledge.ACK_NO).error(code ==
                JMQCode.SUCCESS ? null : code.getMessage());

        return new Command(builder.build(), null);
    }

    /**
     * 构造布尔应答
     *
     * @param requestId 请求ID
     * @param code      代码
     * @param message   消息
     * @return 布尔应答
     */
    public static Command createBooleanAck(final int requestId, final int code, final String message) {
        JMQHeader.Builder builder = JMQHeader.Builder.create().direction(Direction.RESPONSE).type(CmdTypes
                .BOOLEAN_ACK).requestId(requestId).status(code).acknowledge(Acknowledge.ACK_NO).error(code ==
                JMQCode.SUCCESS.getCode() ? null : message);
        return new Command(builder.build(), null);
    }

    /**
     * 构造Command应答
     *
     * @param request   收到的请求对象
     * @param response  生成的响应Payload
     * @return Command类型的响应对象
     */
    public static Command createResponse(final Command request, final JMQPayload response) {
        JMQHeader header = new JMQHeader(Direction.RESPONSE, response.type(), request.getHeader().getRequestId(), JMQCode.SUCCESS.getCode(), null);
        header.setAcknowledge(Acknowledge.ACK_NO);
        return new Command(header, response);

    }

    public static boolean checkSuccess(Command command){
        JMQHeader header = (JMQHeader) command.getHeader();
        return header.getStatus() == JMQCode.SUCCESS.getCode();
    }
}
