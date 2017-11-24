package com.ipd.jmq.toolkit.config.annotation;

import java.lang.annotation.*;

/**
 * 嵌套绑定
 * Created by hexiaofeng on 15-7-20.
 */
@Target({ElementType.FIELD, ElementType.TYPE})
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface Binding {

}
