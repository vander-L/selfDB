package org.yujiabin.selfDB.exception;

public class UpdateXidFileException extends RuntimeException{
    public UpdateXidFileException() {
    }

    public UpdateXidFileException(String message) {
        super(message);
    }

    public UpdateXidFileException(String message, Throwable cause) {
        super(message, cause);
    }

    public UpdateXidFileException(Throwable cause) {
        super(cause);
    }

    public UpdateXidFileException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
