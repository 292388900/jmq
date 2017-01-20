package com.ipd.jmq.toolkit.validate.annotation;

import java.lang.annotation.*;

/**
 * 字符串、数组、集合、散列大小
 * Created by hexiaofeng on 15-7-20.
 */
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface Size {

    /**
     * 最小值
     *
     * @return 最小值
     */
    int min() default 0;

    /**
     * 最大值
     *
     * @return 最大值
     */
    int max() default Integer.MAX_VALUE;

    String message() default "";

}
