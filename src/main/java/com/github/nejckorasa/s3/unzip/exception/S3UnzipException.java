package com.github.nejckorasa.s3.unzip.exception;

public class S3UnzipException extends RuntimeException {

    public S3UnzipException(String message) {
        super(message);
    }

    public S3UnzipException(String message, Throwable cause) {
        super(message, cause);
    }
}
