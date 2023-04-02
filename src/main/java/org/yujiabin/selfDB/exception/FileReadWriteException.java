package org.yujiabin.selfDB.exception;

public class FileReadWriteException extends RuntimeException{
    public FileReadWriteException() {
    }

    public FileReadWriteException(String message) {
        super(message);
    }

    public FileReadWriteException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileReadWriteException(Throwable cause) {
        super(cause);
    }

    public FileReadWriteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
