package org.yujiabin.selfDB.exception;

public class CacheFullException extends RuntimeException{
    public CacheFullException() {
    }

    public CacheFullException(String message) {
        super(message);
    }

    public CacheFullException(String message, Throwable cause) {
        super(message, cause);
    }

    public CacheFullException(Throwable cause) {
        super(cause);
    }

    public CacheFullException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
