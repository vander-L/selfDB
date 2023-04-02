package org.yujiabin.selfDB.exception;

public class DatabaseBusyException extends RuntimeException{
    public DatabaseBusyException() {
    }

    public DatabaseBusyException(String message) {
        super(message);
    }

    public DatabaseBusyException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseBusyException(Throwable cause) {
        super(cause);
    }

    public DatabaseBusyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
