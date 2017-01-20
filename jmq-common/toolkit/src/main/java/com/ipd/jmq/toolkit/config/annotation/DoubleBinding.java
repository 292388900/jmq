package com.ipd.jmq.toolkit.config.annotation;

import com.ipd.jmq.toolkit.validate.annotation.DoubleRange;

import java.lang.annotation.*;

/**
 * 绑定上下文
 * Created by hexiaofeng on 15-7-20.
 */
@Target(ElementType.FIELD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface DoubleBinding {

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
    double defaultValue() default (double) 0;

    /**
     * 数值范围
     *
     * @return 数值范围
     */
    DoubleRange range() default @DoubleRange;

}
