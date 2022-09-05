package com.github.nejckorasa.s3.unzip.strategy;

import com.amazonaws.services.s3.AmazonS3;
import com.github.nejckorasa.s3.unzip.UnzipTask;

public interface UnzipStrategy {

    void unzip(UnzipTask unzipTask, AmazonS3 s3Client);
}
