package com.ipd.jmq.client.config;

public class MqNsException extends RuntimeException {

    /**  */
    private static final long serialVersionUID = 4283390130473640414L;

    /**
     *
     */
    public MqNsException() {
        super();
    }

    /**
     * @param message
     * @param cause
     */
    public MqNsException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     */
    public MqNsException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public MqNsException(Throwable cause) {
        super(cause);
    }

}
