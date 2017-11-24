package com.ipd.jmq.toolkit.config.annotation;

import java.lang.annotation.*;

/**
 * 绑定上下文
 * Created by hexiaofeng on 15-7-20.
 */
@Target(ElementType.FIELD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface StringBinding {

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

}
