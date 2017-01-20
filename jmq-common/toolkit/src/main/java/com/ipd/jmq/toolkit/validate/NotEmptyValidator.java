package com.ipd.jmq.toolkit.validate;

import com.ipd.jmq.toolkit.reflect.Reflect;
import com.ipd.jmq.toolkit.validate.annotation.NotEmpty;

import java.lang.annotation.Annotation;

/**
 * 验证不能为空，支持字符序列，集合，散列，数组
 * Created by hexiaofeng on 16-5-10.
 */
public class NotEmptyValidator implements Validator {

    public static final NotEmptyValidator INSTANCE = new NotEmptyValidator();

    @Override
    public void validate(final Object target, final Annotation annotation, final Value value) throws ValidateException {
        NotEmpty notEmpty = (NotEmpty) annotation;
        int size = Reflect.size(value.type, value.value);
        if (size == 0) {
            if (notEmpty.message() == null || notEmpty.message().isEmpty()) {
                throw new ValidateException(String.format("%s must not be empty.", value.name));
            }
            throw new ValidateException(String.format(notEmpty.message(), value.name));
        }
    }
}
