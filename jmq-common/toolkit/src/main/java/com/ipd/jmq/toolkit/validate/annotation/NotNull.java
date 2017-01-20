package com.ipd.jmq.toolkit.validate.annotation;

import java.lang.annotation.*;

/**
 * 不能为空验证
 * Created by hexiaofeng on 15-7-20.
 */
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface NotNull {

    String message() default "";

}
