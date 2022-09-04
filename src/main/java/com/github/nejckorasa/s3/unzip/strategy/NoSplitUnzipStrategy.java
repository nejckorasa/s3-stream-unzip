package com.github.nejckorasa.s3.unzip.strategy;

import com.amazonaws.services.s3.AmazonS3;
import com.github.nejckorasa.s3.unzip.exception.S3UnzipException;
import com.github.nejckorasa.s3.upload.S3MultipartUpload;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static com.amazonaws.services.s3.internal.Constants.MB;

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
    public void unzip(UnzipTask unzipTask, AmazonS3 s3Client) {
        String filename = unzipTask.filename();
        long compressedSizeInMB = unzipTask.compressedSizeInMB();
        long sizeInMB = unzipTask.sizeInMB();

        String key = unzipTask.key();
        var multipartUpload = new S3MultipartUpload(unzipTask.bucketName(), key, s3Client, config);
        multipartUpload.initializeUpload();

        log.info("Unzipping {}, compressed: {} MB, extracted: {} MB to {}", filename, compressedSizeInMB, sizeInMB, key);

        try {
            int bytesRead;
            long allBytesRead = 0;
            long uploadBytes = 0;
            long partsCount = 0;

            byte[] data = new byte[uploadPartBytesLimit];
            var outputStream = new ByteArrayOutputStream();

            while ((bytesRead = unzipTask.inputStream().read(data, 0, data.length)) != -1) {
                outputStream.write(data, 0, bytesRead);

                if (uploadBytes < uploadPartBytesLimit) {
                    uploadBytes += bytesRead;
                    continue;
                }

                partsCount += 1;
                allBytesRead += bytesRead;

                long allMBRead = allBytesRead * MB;

                log.info("Uploading part [{}] for file: {} - Read {} MB out of {} MB", partsCount, filename, allMBRead, sizeInMB);

                multipartUpload.uploadPart(new ByteArrayInputStream(outputStream.toByteArray()));
                outputStream.reset();
                uploadBytes = 0;
            }

            multipartUpload.uploadFinalPart(new ByteArrayInputStream(outputStream.toByteArray()));

        } catch (Throwable t) {
            multipartUpload.abort();
            throw new S3UnzipException("Failed to unzip " + filename, t);
        }
    }
}