package ru.eremin.common.exception;

public class RetryableEventException extends RuntimeException {

    public RetryableEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
