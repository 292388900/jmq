package com.ipd.jmq.toolkit.config.annotation;

import java.lang.annotation.*;

/**
 * 绑定上下文
 * Created by hexiaofeng on 15-7-20.
 */
@Target(ElementType.FIELD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface ObjectBinding {

    /**
     * 键
     *
     * @return 键
     */
    String key() default "";

}
