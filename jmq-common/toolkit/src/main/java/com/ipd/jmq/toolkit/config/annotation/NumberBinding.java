package com.ipd.jmq.toolkit.config.annotation;

import com.ipd.jmq.toolkit.validate.annotation.Range;

import java.lang.annotation.*;

/**
 * 数字
 * Created by hexiaofeng on 15-7-20.
 */
@Target(ElementType.FIELD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface NumberBinding {

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
    long defaultValue() default (long) 0;

    /**
     * 数值范围
     *
     * @return 数值范围
     */
    Range range() default @Range;

}
