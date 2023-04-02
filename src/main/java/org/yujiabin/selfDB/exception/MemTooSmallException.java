package org.yujiabin.selfDB.exception;

public class MemTooSmallException extends RuntimeException{
    public MemTooSmallException() {
    }

    public MemTooSmallException(String message) {
        super(message);
    }

    public MemTooSmallException(String message, Throwable cause) {
        super(message, cause);
    }

    public MemTooSmallException(Throwable cause) {
        super(cause);
    }

    public MemTooSmallException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
