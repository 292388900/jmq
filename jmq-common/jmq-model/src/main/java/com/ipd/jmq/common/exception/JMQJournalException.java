package com.ipd.jmq.common.exception;

/**
 * 日志异常
 */
public class JMQJournalException extends JMQException {
    // 文件ID
    private long fileId;
    // 文件中的位置
    private int position;
    // 全局位置
    private long offset;

    public JMQJournalException(long fileId, int position) {
        super(JMQCode.SE_INVALID_JOURNAL, fileId, position);
        this.fileId = fileId;
        this.position = position;
        this.offset = fileId + position;
    }

    public JMQJournalException(long fileId, int position, Throwable cause) {
        super(JMQCode.SE_INVALID_JOURNAL, cause, fileId, position);
        this.fileId = fileId;
        this.position = position;
        this.offset = fileId + position;
    }

    public long getFileId() {
        return fileId;
    }

    public int getPosition() {
        return position;
    }

    public long getOffset() {
        return offset;
    }

}
