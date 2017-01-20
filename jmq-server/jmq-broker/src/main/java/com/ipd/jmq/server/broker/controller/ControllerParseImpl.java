package com.ipd.jmq.server.broker.controller;

/**
 * Created by zhangkepeng on 16-5-18.
 */
public class ControllerParseImpl implements ControllerParse {

    private Object parseContent;

    public Object getParseContent() {
        return this.parseContent;
    }

    public void setParseContent(Object object) {
        this.parseContent = object;
    }
}
