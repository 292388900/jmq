package com.ipd.jmq.toolkit.validate.annotation;

import java.lang.annotation.*;

/**
 * 数字范围检查
 * Created by hexiaofeng on 15-7-20.
 */
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface Range {

    /**
     * 最小值
     *
     * @return 最小值
     */
    long min() default Long.MIN_VALUE;

    /**
     * 最大值
     *
     * @return 最大值
     */
    long max() default Long.MAX_VALUE;

    /**
     * 错误提示
     *
     * @return 消息
     */
    String message() default "";
}
