package com.ipd.jmq.toolkit.config;

import com.ipd.jmq.toolkit.config.annotation.*;
import com.ipd.jmq.toolkit.reflect.ReflectException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

/**
 * 绑定器
 * Created by hexiaofeng on 16-8-29.
 */
public class Binders {
    /**
     * 绑定上下文
     *
     * @param context 上下文
     * @param target  对象
     * @throws ReflectException
     */
    public static void bind(final Context context, final Object target) throws ReflectException {
        if (context == null || target == null) {
            return;
        }
        Class<?> clazz = target.getClass();
        Field[] fields;
        Annotation[] annotations;
        Binder binder;
        while (clazz != null && clazz != Object.class) {
            fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                annotations = field.getDeclaredAnnotations();
                if (annotations != null) {
                    for (Annotation annotation : annotations) {
                        binder = getBinder(annotation);
                        if (binder != null) {
                            binder.bind(field, annotation, target, context);
                        }
                    }
                }
            }
            clazz = clazz.getSuperclass();
        }
    }

    /**
     * 获取验证器
     *
     * @param annotation 声明
     * @return 验证器
     */
    protected static Binder getBinder(final Annotation annotation) {
        Binder binder = null;
        if (annotation instanceof BooleanBinding) {
            binder = BooleanBinder.INSTANCE;
        } else if (annotation instanceof Binding) {
            binder = BindingBinder.INSTANCE;
        } else if (annotation instanceof DateBinding) {
            binder = DateBinder.INSTANCE;
        } else if (annotation instanceof DoubleBinding) {
            binder = DoubleBinder.INSTANCE;
        } else if (annotation instanceof NumberBinding) {
            binder = NumberBinder.INSTANCE;
        } else if (annotation instanceof ObjectBinding) {
            binder = ObjectBinder.INSTANCE;
        } else if (annotation instanceof StringBinding) {
            binder = StringBinder.INSTANCE;
        }
        return binder;
    }

}
