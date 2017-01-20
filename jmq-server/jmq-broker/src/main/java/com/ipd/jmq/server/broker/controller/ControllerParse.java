package com.ipd.jmq.server.broker.controller;

/**
 * Created by zhangkepeng on 16-5-18.
 */
public interface ControllerParse {

    // 设置解析后内容
    void setParseContent(Object object);

    // 获取解析后内容
    Object getParseContent();
}
