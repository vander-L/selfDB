package org.yujiabin.selfDB.exception;

public class DuplicatedTableException extends RuntimeException{
    public DuplicatedTableException() {
    }

    public DuplicatedTableException(String message) {
        super(message);
    }

    public DuplicatedTableException(String message, Throwable cause) {
        super(message, cause);
    }

    public DuplicatedTableException(Throwable cause) {
        super(cause);
    }

    public DuplicatedTableException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
