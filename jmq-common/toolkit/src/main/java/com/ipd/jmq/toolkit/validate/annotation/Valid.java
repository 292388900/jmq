package com.ipd.jmq.toolkit.validate.annotation;

import java.lang.annotation.*;

/**
 * 嵌套验证，支持对象、集合和数组
 * Created by hexiaofeng on 15-7-20.
 */
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.TYPE})
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface Valid {

}
