package org.yujiabin.selfDB.exception;

public class BadLogFileException extends RuntimeException{
    public BadLogFileException() {
    }

    public BadLogFileException(String message) {
        super(message);
    }

    public BadLogFileException(String message, Throwable cause) {
        super(message, cause);
    }

    public BadLogFileException(Throwable cause) {
        super(cause);
    }

    public BadLogFileException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
