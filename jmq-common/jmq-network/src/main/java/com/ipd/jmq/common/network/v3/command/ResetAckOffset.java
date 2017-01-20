package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.cluster.OffsetItem;
import com.ipd.jmq.toolkit.lang.Preconditions;

/**
 * 重置消费位置.
 *
 * @author lindeqiang
 * @since 2016/9/14 9:59
 */
public class ResetAckOffset extends JMQPayload {
    // 需要重置的消息位置
    private OffsetItem[] items;

    public ResetAckOffset() {
    }

    public ResetAckOffset(OffsetItem[] setters) {
        this.items = setters;
    }

    public OffsetItem[] getItems() {
        return items;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(items != null && items.length > 0, "items can not be empty.");
    }

    public void setItems(OffsetItem[] items) {
        this.items = items;
    }

    @Override
    public int type() {
        return CmdTypes.RESET_ACK_OFFSET;
    }
}
