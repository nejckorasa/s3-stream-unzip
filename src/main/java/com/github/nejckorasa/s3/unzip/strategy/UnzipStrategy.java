package com.github.nejckorasa.s3.unzip.strategy;

import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public interface UnzipStrategy {
    void validate();

    void unzip(String bucketName, String outputPrefix, ZipInputStream zipInputStream, ZipEntry zipEntry);
}
