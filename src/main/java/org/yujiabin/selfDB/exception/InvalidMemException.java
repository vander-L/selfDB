package org.yujiabin.selfDB.exception;

public class InvalidMemException extends RuntimeException{
    public InvalidMemException() {
    }

    public InvalidMemException(String message) {
        super(message);
    }

    public InvalidMemException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidMemException(Throwable cause) {
        super(cause);
    }

    public InvalidMemException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
