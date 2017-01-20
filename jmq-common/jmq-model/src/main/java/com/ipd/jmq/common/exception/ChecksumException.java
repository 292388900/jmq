package com.ipd.jmq.common.exception;

import java.io.IOException;

/**
 * 校验和异常
 */
public class ChecksumException extends IOException {

    public ChecksumException() {
    }

    public ChecksumException(String message) {
        super(message);
    }

    public ChecksumException(String message, Throwable cause) {
        super(message, cause);
    }

    public ChecksumException(Throwable cause) {
        super(cause);
    }
}
