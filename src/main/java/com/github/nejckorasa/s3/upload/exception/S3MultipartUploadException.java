package com.github.nejckorasa.s3.upload.exception;

public class S3MultipartUploadException extends RuntimeException {

    public S3MultipartUploadException() {
        super();
    }

    public S3MultipartUploadException(String message) {
        super(message);
    }

    public S3MultipartUploadException(String message, Throwable cause) {
        super(message, cause);
    }
}
