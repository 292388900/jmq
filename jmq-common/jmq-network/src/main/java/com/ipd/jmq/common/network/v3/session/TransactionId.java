package com.ipd.jmq.common.network.v3.session;

import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事务ID
 */
public class TransactionId {

    public static AtomicLong TRANSACTION_ID = new AtomicLong(0);
    // 生产者ID
    private ProducerId producerId;
    // 序号
    private long sequence;
    // 事务ID
    private String transactionId;

    /**
     * 构造函数
     *
     * @param producerId 生产者ID
     */
    public TransactionId(final ProducerId producerId) {
        setup(producerId, 0);
    }

    /**
     * 构造函数
     *
     * @param producerId 生产者ID
     * @param sequence   序号
     */
    public TransactionId(final ProducerId producerId, final long sequence) {
        setup(producerId, sequence);
    }

    public TransactionId(ProducerId producerId, String transactionId) {
        if (producerId == null) {
            throw new IllegalArgumentException("producerId must not be null");
        }
        this.producerId = producerId;
        // 在构造函数中创建，防止延迟加载并发问题
        this.transactionId = transactionId;
        int pos = transactionId.lastIndexOf(":");
        if(pos >0){
            sequence = Long.valueOf(transactionId.substring(pos+1),10);
        }
    }

    /**
     * 构造函数
     *
     * @param parts 字符串分割
     */
    public TransactionId(final String[] parts) {
        setup(new ProducerId(parts), Long.parseLong(parts[5]));
    }

    /**
     * 构造函数
     *
     * @param transactionId 事务ID字符串
     */
    public TransactionId(final String transactionId) {
        if (transactionId == null || transactionId.isEmpty()) {
            throw new IllegalArgumentException("transactionId can not be empty");
        }
        String[] parts = new String[]{null, null, null, null, null, null, null};
        int index = 0;
        StringTokenizer tokenizer = new StringTokenizer(transactionId, "-");
        while (tokenizer.hasMoreTokens()) {
            parts[index++] = tokenizer.nextToken();
            if (index >= parts.length) {
                break;
            }
        }
        if (index < parts.length) {
            throw new IllegalArgumentException("transactionId is invalid.");
        }
        setup(new ProducerId(parts), Long.parseLong(parts[parts.length - 1]));
    }

    /**
     * 初始化
     *
     * @param producerId 生产者ID
     * @param sequence   序号
     */
    protected void setup(final ProducerId producerId, final long sequence) {
        if (producerId == null) {
            throw new IllegalArgumentException("producerId must not be null");
        }

        long seq = sequence;
        if (seq <= 0) {
            seq = TRANSACTION_ID.incrementAndGet();
        }
        this.producerId = producerId;
        this.sequence = seq;
        // 在构造函数中创建，防止延迟加载并发问题
        this.transactionId = producerId.getProducerId() + "-" + seq;
    }

    public ProducerId getProducerId() {
        return this.producerId;
    }

    public long getSequence() {
        return this.sequence;
    }

    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TransactionId that = (TransactionId) o;

        if (sequence != that.sequence) {
            return false;
        }
        if (producerId != null ? !producerId.equals(that.producerId) : that.producerId != null) {
            return false;
        }
        if (transactionId != null ? !transactionId.equals(that.transactionId) : that.transactionId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = producerId != null ? producerId.hashCode() : 0;
        result = 31 * result + (int) (sequence ^ (sequence >>> 32));
        result = 31 * result + (transactionId != null ? transactionId.hashCode() : 0);
        return result;
    }

    public String toString() {
        return getTransactionId();


    }

}