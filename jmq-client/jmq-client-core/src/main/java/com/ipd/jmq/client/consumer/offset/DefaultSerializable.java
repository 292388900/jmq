package com.ipd.jmq.client.consumer.offset;


import com.alibaba.fastjson.JSON;

/**
 * Created by zhangkepeng on 15-8-24.
 */
public abstract class DefaultSerializable {

    public String objectToJsonString(final boolean prettyFormat) {
        return objectToJsonString(this, prettyFormat);
    }


    public static String objectToJsonString(final Object obj, boolean prettyFormat) {
        return JSON.toJSONString(obj, prettyFormat);
    }

    public static <T> T jsonStringToObject(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }

}
