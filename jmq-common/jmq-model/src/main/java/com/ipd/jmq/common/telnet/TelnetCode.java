package com.ipd.jmq.common.telnet;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangkepeng on 16-11-29.
 *
 * Telnet定义的错误编码
 */
public class TelnetCode {

    public static Map<Short, String> ERROR_TEXT = new HashMap<>();

    // 未知错误
    public static final short UNKNOWN = -1;
    // 成功
    public static final short NO_ERROR = 0;
    // 未认证
    public static final short NO_AUTH = 1;
    // 认证失败
    public static final short AUTH_ERROR = 2;
    // 认证成功
    public static final short AUTH_SUCCESS = 3;
    // 命令参数错误
    public static final short PARAM_ERROR = 4;
    // 调用异常
    public static final short INVOCATION_EXCEPTION = 5;
    // 访问异常
    public static final short ACCESS_EXCEPTION = 6;
    // 未找到方法
    public static final short NO_METHOD_EXCEPTION = 7;
    // 未找到本地分片
    public static final short NO_LOCAL_BROKER_EXCEPTION = 8;
    // 重启参数错误
    public static final short SYSTEM_PARA_ERROR = 9;
    // 重启已提交
    public static final short SYSTEM_SUBMITTED = 10;
    // 重启超时
    public static final short SYSTEM_TIME_OUT = 11;
    // 数据压缩或解压错误
    public static final short COMPRESS_ERROR = 12;
    // 注册中心错误
    public static final short REGISTRY_ERROR = 13;

    static {
        ERROR_TEXT.put(UNKNOWN, "unknow error");
        ERROR_TEXT.put(NO_ERROR, "success");
        ERROR_TEXT.put(NO_AUTH, "no auth, auth firstly");
        ERROR_TEXT.put(AUTH_ERROR, "auth error, check user and password");
        ERROR_TEXT.put(AUTH_SUCCESS, "auth success");
        ERROR_TEXT.put(PARAM_ERROR, "parameter exception, help command");
        ERROR_TEXT.put(INVOCATION_EXCEPTION, "invocate method exception");
        ERROR_TEXT.put(ACCESS_EXCEPTION, "method access exception");
        ERROR_TEXT.put(NO_METHOD_EXCEPTION, "no method exception");
        ERROR_TEXT.put(NO_LOCAL_BROKER_EXCEPTION, "no local broker exception");
        ERROR_TEXT.put(COMPRESS_ERROR, "compress exception");
        ERROR_TEXT.put(REGISTRY_ERROR, "registry exception");
    }
}
