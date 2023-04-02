package org.yujiabin.selfDB.exception;

public class CreateFileException extends RuntimeException{
    public CreateFileException() {
    }

    public CreateFileException(String message) {
        super(message);
    }

    public CreateFileException(String message, Throwable cause) {
        super(message, cause);
    }

    public CreateFileException(Throwable cause) {
        super(cause);
    }

    public CreateFileException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
