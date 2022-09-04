package com.github.nejckorasa.s3.unzip.strategy;

import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.amazonaws.services.s3.internal.Constants.MB;

public record UnzipTask(String bucketName, String outputPrefix, ZipInputStream inputStream, ZipEntry zipEntry) {
    public String filename() {
        return zipEntry.getName();
    }

    public long compressedSizeInMB() {
        return zipEntry.getCompressedSize() * MB;
    }

    public long sizeInMB() {
        return zipEntry.getSize() * MB;
    }

    public String key() {
        return outputPrefix + filename();
    }
}