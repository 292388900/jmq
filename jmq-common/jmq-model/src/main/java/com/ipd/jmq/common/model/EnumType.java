package com.ipd.jmq.common.model;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface EnumType {

    /**
     * 返回枚举类型
     *
     * @return 枚举类型
     */
    Class<? extends EnumItem> value();
}