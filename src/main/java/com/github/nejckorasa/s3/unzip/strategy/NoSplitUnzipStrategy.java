package com.github.nejckorasa.s3.unzip.strategy;

import com.amazonaws.services.s3.AmazonS3;
import com.github.nejckorasa.s3.unzip.S3UnzipException;
import com.github.nejckorasa.s3.unzip.S3ZipFile;
import com.github.nejckorasa.s3.upload.S3MultipartUpload;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;

import static com.amazonaws.services.s3.internal.Constants.MB;

/**
 * Unzips and uploads a file without splitting (sharding) - it creates a 1:1 mapping between zipped and unzipped file
 *
 * <p> File is read in bytes. This strategy should ideally be used for smaller files.
 *
 * <p> Utilizes stream download and multipart upload - unzipping is achieved without keeping all data in memory or writing to disk.
 */
@Slf4j
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class NoSplitUnzipStrategy implements UnzipStrategy {

    @NonNull
    @With
    private int uploadPartBytesLimit = 20 * MB;

    @NonNull
    private S3MultipartUpload.Config config = S3MultipartUpload.Config.DEFAULT;

    public NoSplitUnzipStrategy(@NonNull S3MultipartUpload.Config config) {
        this.config = config;
    }

    @Override
    public void unzip(S3ZipFile zipFile, AmazonS3 s3Client) {
        String filename = zipFile.filename();
        long compressedSize = zipFile.compressedSize();
        long size = zipFile.size();

        String key = zipFile.key();
        var s3MultipartUpload = new S3MultipartUpload(zipFile.bucketName(), key, s3Client, config);
        s3MultipartUpload.initialize();

        log.info("Unzipping {}, compressed: {} bytes, extracted: {} bytes to {}", filename, compressedSize, size, key);
        try {
            int bytesRead;
            long allBytesRead = 0;
            long uploadPartBytes = 0;
            long partNumber = 0;

            byte[] data = new byte[uploadPartBytesLimit];
            var outputStream = new ByteArrayOutputStream();

            while ((bytesRead = zipFile.inputStream().read(data, 0, data.length)) != -1) {
                outputStream.write(data, 0, bytesRead);

                if (uploadPartBytes < uploadPartBytesLimit) {
                    uploadPartBytes += bytesRead;
                    continue;
                }

                partNumber += 1;
                allBytesRead += bytesRead;

                log.debug("Uploading part [{}] for file: {} - Read {} bytes out of {} bytes", partNumber, filename, allBytesRead, size);

                s3MultipartUpload.uploadPart(outputStream.toByteArray());
                outputStream.reset();
                uploadPartBytes = 0;
            }

            s3MultipartUpload.uploadFinalPart(outputStream.toByteArray());
            log.info("Unzipped and uploaded file: {} in {} parts", filename, partNumber);

        } catch (Throwable t) {
            s3MultipartUpload.abort();
            throw new S3UnzipException("Failed to unzip " + filename, t);
        }
    }
}
