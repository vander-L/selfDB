package org.yujiabin.selfDB.exception;

public class GetFromXidFileException extends RuntimeException{
    public GetFromXidFileException() {
    }

    public GetFromXidFileException(String message) {
        super(message);
    }

    public GetFromXidFileException(String message, Throwable cause) {
        super(message, cause);
    }

    public GetFromXidFileException(Throwable cause) {
        super(cause);
    }

    public GetFromXidFileException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
