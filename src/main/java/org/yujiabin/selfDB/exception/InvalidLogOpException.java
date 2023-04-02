package org.yujiabin.selfDB.exception;

public class InvalidLogOpException extends RuntimeException{

    public InvalidLogOpException() {
    }

    public InvalidLogOpException(String message) {
        super(message);
    }

    public InvalidLogOpException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidLogOpException(Throwable cause) {
        super(cause);
    }

    public InvalidLogOpException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
