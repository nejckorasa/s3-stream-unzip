package com.github.nejckorasa.s3.upload;

public class S3MultipartUploadException extends RuntimeException {

    public S3MultipartUploadException() {
        super();
    }

    public S3MultipartUploadException(String message, Throwable cause) {
        super(message, cause);
    }
}
