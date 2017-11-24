package com.ipd.jmq.server.broker.handler.telnet.utils;

import java.lang.reflect.Method;
import java.util.Date;

/**
 * Created by zhangkepeng on 16-12-2.
 */
public class ReflectUtils {

    public static Method findMethod(Class service, String methodName, Object[] args) {
        Method[] methods = service.getMethods();
        Method invokeMethod = null;
        for (Method m : methods) {
            if (m.getName().equals(methodName) && (args == null || (args != null && m.getParameterTypes().length == args.length))) {
                if (invokeMethod != null) { // 重载
                    if (isMatch(invokeMethod.getParameterTypes(), args)) {
                        invokeMethod = m;
                        break;
                    }
                } else {
                    invokeMethod = m;
                }
            }
        }
        return invokeMethod;
    }

    private static boolean isMatch(Class<?>[] types, Object[] args) {
        if (types.length != args.length) {
            return false;
        }
        for (int i = 0; i < types.length; i ++) {
            Class<?> type = types[i];
            Object arg = args[i];
            if (isPrimitiveType(arg.getClass())) {
                if (!isPrimitiveType(type)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isPrimitiveType(Class<?> clazz) {
        return clazz.isPrimitive() // 基本类型
                // 基本类型的对象
                || Boolean.class == clazz
                || Character.class == clazz
                || Number.class.isAssignableFrom(clazz)
                // string 或者 date
                || String.class == clazz
                || Date.class.isAssignableFrom(clazz);
    }
}
