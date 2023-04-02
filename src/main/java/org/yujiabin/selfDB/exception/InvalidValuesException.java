package org.yujiabin.selfDB.exception;

public class InvalidValuesException extends RuntimeException{
    public InvalidValuesException() {
    }

    public InvalidValuesException(String message) {
        super(message);
    }

    public InvalidValuesException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidValuesException(Throwable cause) {
        super(cause);
    }

    public InvalidValuesException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
