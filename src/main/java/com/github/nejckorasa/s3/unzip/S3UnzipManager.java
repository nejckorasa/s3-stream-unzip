package com.github.nejckorasa.s3.unzip;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.nejckorasa.s3.unzip.exception.S3UnzipException;
import com.github.nejckorasa.s3.unzip.strategy.UnzipStrategy;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipInputStream;

import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.joining;

@Slf4j
@Builder
public class S3UnzipManager {

    private UnzipStrategy unzipStrategy;

    @Builder.Default
    private List<String> contentTypes = List.of("application/zip");

    @NonNull
    private final AmazonS3 s3Client;

    public void unzipObject(S3Object s3Object, String outputPrefix) {
        unzipStrategy.validate();

        unzip(s3Object, outputPrefix);
    }

    public void unzipObjects(String bucket, String inputPrefix, String outputPrefix) {
        unzipStrategy.validate();

        findObjectSummaries(bucket, inputPrefix)
                .forEach(objSum -> getObjectIfZip(bucket, objSum).ifPresent(obj -> unzip(obj, outputPrefix)));
    }

    public void unzipObjectsKeyContaining(String bucket, String inputPrefix, String keyMatchingString, String outputPrefix) {
        unzipStrategy.validate();

        findObjectSummaries(bucket, inputPrefix).stream()
                .filter(objSum -> objSum.getKey().contains(keyMatchingString))
                .forEach(objSum -> getObjectIfZip(bucket, objSum).ifPresent(obj -> unzip(obj, outputPrefix)));
    }

    public void unzipObjectsKeyMatching(String bucket, String inputPrefix, String keyMatchingString, String outputPrefix) {
        unzipStrategy.validate();

        findObjectSummaries(bucket, inputPrefix).stream()
                .filter(objSum -> objSum.getKey().matches(keyMatchingString))
                .forEach(objSum -> getObjectIfZip(bucket, objSum).ifPresent(obj -> unzip(obj, outputPrefix)));
    }

    private void unzip(S3Object s3Object, String outputPrefix) {
        if (!hasValidContentType(s3Object)) {
            throw new S3UnzipException("s3Object has invalid type: " + s3Object.getObjectMetadata().getContentType());
        }

        if (!outputPrefix.endsWith("/")) {
            outputPrefix += "/";
        }

        String bucketName = s3Object.getBucketName();

        try (var zipInputStream = new ZipInputStream(s3Object.getObjectContent())) {
            var zipEntry = zipInputStream.getNextEntry();
            while (zipEntry != null) {
                var start = currentTimeMillis();
                unzipStrategy.unzip(bucketName, outputPrefix, zipInputStream, zipEntry);
                log.info("Unzipped {} in {} s", zipEntry.getName(), (currentTimeMillis() - start) / 1000);
                zipEntry = zipInputStream.getNextEntry();
            }
            zipInputStream.closeEntry();

        } catch (IOException e) {
            throw new S3UnzipException("Failed reading zip input stream", e);
        }
    }

    private List<S3ObjectSummary> findObjectSummaries(String bucket, String inputPrefix) {
        var s3Objects = s3Client.listObjects(bucket, inputPrefix).getObjectSummaries();
        log.debug("Found s3Objects: {}", s3Objects.stream().map(S3ObjectSummary::getKey).collect(joining(", ", "[", "]")));
        return s3Objects;
    }

    private Optional<S3Object> getObjectIfZip(String bucket, S3ObjectSummary objectSummary) {
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, objectSummary.getKey()));
        if (hasValidContentType(s3Object)) {
            log.debug("Found zip s3Object: {}", s3Object.getKey());
            return Optional.of(s3Object);
        } else {
            log.debug("Skipping s3Object: {} - content type does not match any of {}",
                    s3Object.getKey(),
                    contentTypes.stream().collect(joining(", ", "[", "]")));
            return Optional.empty();
        }
    }

    private boolean hasValidContentType(S3Object s3Object) {
        return contentTypes.contains(s3Object.getObjectMetadata().getContentType());
    }
}
