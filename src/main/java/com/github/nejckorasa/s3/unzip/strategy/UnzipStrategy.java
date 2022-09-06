package com.github.nejckorasa.s3.unzip.strategy;

import com.amazonaws.services.s3.AmazonS3;
import com.github.nejckorasa.s3.unzip.S3ZipFile;

public interface UnzipStrategy {

    void unzip(S3ZipFile zipFile, AmazonS3 s3Client);
}
