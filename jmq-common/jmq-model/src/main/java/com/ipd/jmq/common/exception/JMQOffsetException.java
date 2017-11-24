package com.ipd.jmq.common.exception;

/**
 * 存储位置无效异常
 */
public class JMQOffsetException extends JMQException {

    // 位置
    private long offset;

    public JMQOffsetException(long offset) {
        super(JMQCode.SE_INVALID_OFFSET,offset);
        this.offset = offset;
    }

    public JMQOffsetException(long offset, String message) {
        super(message, JMQCode.SE_INVALID_OFFSET.getCode());
        this.offset = offset;
    }

    public JMQOffsetException(long offset, Throwable cause) {
        super(cause, JMQCode.SE_INVALID_OFFSET.getCode());
        this.offset = offset;
    }

    public JMQOffsetException(long offset, String message, Throwable cause) {
        super(message, cause, JMQCode.SE_INVALID_OFFSET.getCode());
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }
}
