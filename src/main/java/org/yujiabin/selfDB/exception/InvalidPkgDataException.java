package org.yujiabin.selfDB.exception;

public class InvalidPkgDataException extends RuntimeException{
    public InvalidPkgDataException() {
    }

    public InvalidPkgDataException(String message) {
        super(message);
    }

    public InvalidPkgDataException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidPkgDataException(Throwable cause) {
        super(cause);
    }

    public InvalidPkgDataException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
