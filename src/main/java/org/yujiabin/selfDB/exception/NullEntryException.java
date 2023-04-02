package org.yujiabin.selfDB.exception;

public class NullEntryException extends RuntimeException{
    public NullEntryException() {
    }

    public NullEntryException(String message) {
        super(message);
    }

    public NullEntryException(String message, Throwable cause) {
        super(message, cause);
    }

    public NullEntryException(Throwable cause) {
        super(cause);
    }

    public NullEntryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
