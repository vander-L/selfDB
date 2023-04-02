package org.yujiabin.selfDB.exception;

public class DataTooLargeException extends RuntimeException{
    public DataTooLargeException() {
    }

    public DataTooLargeException(String message) {
        super(message);
    }

    public DataTooLargeException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataTooLargeException(Throwable cause) {
        super(cause);
    }

    public DataTooLargeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
