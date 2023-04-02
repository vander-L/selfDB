package org.yujiabin.selfDB.exception;

public class ConcurrentUpdateException extends RuntimeException{
    public ConcurrentUpdateException() {
    }

    public ConcurrentUpdateException(String message) {
        super(message);
    }

    public ConcurrentUpdateException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConcurrentUpdateException(Throwable cause) {
        super(cause);
    }

    public ConcurrentUpdateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
