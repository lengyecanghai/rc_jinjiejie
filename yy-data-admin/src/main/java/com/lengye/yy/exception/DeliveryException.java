package com.lengye.yy.exception;


/**
 * @author lengye
 */
public class DeliveryException extends Exception {
    public DeliveryException(String message) {
        super(message);
    }

    public DeliveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
