package com.ipd.jmq.toolkit.config.annotation;

import java.lang.annotation.*;

/**
 * 绑定日期上下文
 * Created by hexiaofeng on 15-7-20.
 */
@Target(ElementType.FIELD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface DateBinding {

    /**
     * 键
     *
     * @return 键
     */
    String key() default "";

    /**
     * 默认值
     *
     * @return 默认值
     */
    String defaultValue() default "";

    /**
     * 日期格式
     *
     * @return 日期格式
     */
    String format() default "yyyy-MM-dd HH:mm:ss";
}
