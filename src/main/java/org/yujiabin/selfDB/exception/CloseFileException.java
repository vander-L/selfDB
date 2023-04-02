package org.yujiabin.selfDB.exception;

public class CloseFileException extends RuntimeException{
    public CloseFileException() {
    }

    public CloseFileException(String message) {
        super(message);
    }

    public CloseFileException(String message, Throwable cause) {
        super(message, cause);
    }

    public CloseFileException(Throwable cause) {
        super(cause);
    }

    public CloseFileException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
