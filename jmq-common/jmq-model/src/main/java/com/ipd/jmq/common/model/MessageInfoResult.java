package com.ipd.jmq.common.model;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: weiqisong
 * Date: 14-7-22
 * Time: 下午4:13
 * To change this template use File | Settings | File Templates.
 */
public class MessageInfoResult {
    private List<MessageInfo> messageInfoList;
    private boolean isCanPage;
    private String startRow;

    public List<MessageInfo> getMessageInfoList() {
        return messageInfoList;
    }

    public void setMessageInfoList(List<MessageInfo> messageInfoList) {
        this.messageInfoList = messageInfoList;
    }

    public boolean isCanPage() {
        return isCanPage;
    }

    public void setCanPage(boolean canPage) {
        isCanPage = canPage;
    }

    public String getStartRow() {
        return startRow;
    }

    public void setStartRow(String startRow) {
        this.startRow = startRow;
    }
}
