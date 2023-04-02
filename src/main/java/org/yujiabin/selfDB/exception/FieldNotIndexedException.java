package org.yujiabin.selfDB.exception;

public class FieldNotIndexedException extends RuntimeException{
    public FieldNotIndexedException() {
    }

    public FieldNotIndexedException(String message) {
        super(message);
    }

    public FieldNotIndexedException(String message, Throwable cause) {
        super(message, cause);
    }

    public FieldNotIndexedException(Throwable cause) {
        super(cause);
    }

    public FieldNotIndexedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
