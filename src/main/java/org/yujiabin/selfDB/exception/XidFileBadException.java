package org.yujiabin.selfDB.exception;

public class XidFileBadException extends RuntimeException{
    public XidFileBadException() {
        super();
    }

    public XidFileBadException(String message) {
        super(message);
    }

    public XidFileBadException(String message, Throwable cause) {
        super(message, cause);
    }

    public XidFileBadException(Throwable cause) {
        super(cause);
    }

    protected XidFileBadException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
