package org.yujiabin.selfDB.exception;

public class NestedTransactionException extends RuntimeException{
    public NestedTransactionException() {
    }

    public NestedTransactionException(String message) {
        super(message);
    }

    public NestedTransactionException(String message, Throwable cause) {
        super(message, cause);
    }

    public NestedTransactionException(Throwable cause) {
        super(cause);
    }

    public NestedTransactionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
