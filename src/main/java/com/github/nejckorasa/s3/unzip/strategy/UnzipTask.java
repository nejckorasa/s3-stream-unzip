package com.github.nejckorasa.s3.unzip.strategy;

import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public record UnzipTask(String bucketName, String outputPrefix, ZipInputStream inputStream, ZipEntry zipEntry) {
    public String filename() {
        return zipEntry.getName();
    }

    public long compressedSize() {
        return zipEntry.getCompressedSize();
    }

    public long size() {
        return zipEntry.getSize();
    }

    public String key() {
        return outputPrefix + filename();
    }
}