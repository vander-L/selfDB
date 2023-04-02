package org.yujiabin.selfDB.exception;

public class DeadLockException extends RuntimeException{
    public DeadLockException() {
    }

    public DeadLockException(String message) {
        super(message);
    }

    public DeadLockException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeadLockException(Throwable cause) {
        super(cause);
    }

    public DeadLockException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
