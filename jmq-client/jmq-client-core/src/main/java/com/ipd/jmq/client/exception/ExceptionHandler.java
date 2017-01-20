package com.ipd.jmq.client.exception;

/**
 * ExceptionHandler
 *
 * @author luoruiheng
 * @since 12/7/16
 */
public class ExceptionHandler {

    public static String getExceptionSolution(String exceptionKeyword) {
        String key = getPropertiesKeyByExceptionKeyword(exceptionKeyword);
        String url = ExceptionsReader.get(key, "unknown.exception");
        String description = ", 处理链接：";

        return description + url;
    }

    private static String getPropertiesKeyByExceptionKeyword(String exceptionKeyword) {
        if (null == exceptionKeyword || exceptionKeyword.isEmpty()) {
            return "unknown.exception";
        }

        String key;
        if (exceptionKeyword.contains("请求发送异常")) {
            key = "request.error";
        } else if (exceptionKeyword.contains("请求超时")) {
            key = "request.timeout";
        } else if (exceptionKeyword.contains("连接超时")) {
            key = "connection.timeout";
        } else if (exceptionKeyword.contains("连接出错")) {
            key = "connection.error";
        } else if (exceptionKeyword.contains("认证失败")) {
            key = "authentication.error";
        } else if (exceptionKeyword.contains("无权限")) {
            key = "no.permission";
        } else if (exceptionKeyword.contains("没有可用集群")) {
            key = "no.cluster";
        } else {
            key = "unknown.exception";
        }

        return key;
    }

}
