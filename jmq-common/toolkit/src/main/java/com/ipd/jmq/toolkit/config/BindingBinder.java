package com.ipd.jmq.toolkit.config;

import com.ipd.jmq.toolkit.config.annotation.Binding;
import com.ipd.jmq.toolkit.reflect.Reflect;
import com.ipd.jmq.toolkit.reflect.ReflectException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

/**
 * 对象绑定
 * Created by hexiaofeng on 16-8-29.
 */
public class BindingBinder implements Binder {
    public static final BindingBinder INSTANCE = new BindingBinder();

    @Override
    public void bind(final Field field, final Annotation annotation, final Object target, final Context context) throws
            ReflectException {
        if (field == null || annotation == null || target == null || context == null || !(annotation instanceof
                Binding)) {
            return;
        }
        // 去掉基本类型
        Class type = field.getType();
        if (!isSupported(type)) {
            return;
        } else {
            Object value = Reflect.get(field, target);
            if (type.isArray()) {
                // 数组
                int length = Array.getLength(value);
                Object obj;
                for (int i = 0; i < length; i++) {
                    obj = Array.get(value, i);
                    if (obj != null && isSupported(obj.getClass())) {
                        Binders.bind(context, obj);
                    }
                }
            } else if (Collection.class.isAssignableFrom(type)) {
                // 集合
                for (Object obj : (Collection) value) {
                    if (obj != null && isSupported(obj.getClass())) {
                        Binders.bind(context, obj);
                    }
                }
            } else {
                Binders.bind(context, value);
            }
        }

    }

    /**
     * 是否支持
     *
     * @param type 类型
     * @return 基本类型标示
     */
    protected boolean isSupported(final Class type) {
        if (type == int.class) {
            return false;
        } else if (type == long.class) {
            return false;
        } else if (type == double.class) {
            return false;
        } else if (type == short.class) {
            return false;
        } else if (type == byte.class) {
            return false;
        } else if (type == boolean.class) {
            return false;
        } else if (Number.class.isAssignableFrom(type)) {
            return false;
        } else if (type == Boolean.class) {
            return false;
        } else if (type == String.class) {
            return false;
        } else if (type == Object.class) {
            return false;
        } else if (Date.class.isAssignableFrom(type)) {
            return false;
        } else if (Map.class.isAssignableFrom(type)) {
            return false;
        }
        return true;
    }
}