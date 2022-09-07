package io.github.nejckorasa.s3.unzip;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.github.nejckorasa.s3.unzip.strategy.UnzipStrategy;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.With;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipInputStream;

import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.joining;

/**
 * Utility for managing unzipping of objects in Amazon S3.
 * <p>
 * Manages unzipping of data in AWS S3 utilizing stream download and multipart upload. Unzipping is achieved without knowing the size beforehand and without keeping it all in memory or writing to disk.
 * <p>
 * Supports different unzip strategies, see {@link UnzipStrategy}
 */
@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class S3UnzipManager {

    @NonNull
    private final UnzipStrategy unzipStrategy;

    @NonNull
    private final AmazonS3 s3Client;

    @With
    private List<String> contentTypes = null;

    public S3UnzipManager(@NonNull AmazonS3 s3Client, @NonNull UnzipStrategy unzipStrategy) {
        this.s3Client = s3Client;
        this.unzipStrategy = unzipStrategy;
    }

    public void unzipObject(S3Object s3Object, String outputPrefix) {
        unzip(s3Object, outputPrefix);
    }

    public void unzipObjects(String bucketName, String inputPrefix, String outputPrefix) {
        findObjectSummaries(bucketName, inputPrefix)
                .forEach(objSum -> getObjectIfZip(bucketName, objSum).ifPresent(obj -> unzip(obj, outputPrefix)));
    }

    public void unzipObjectsKeyContaining(String bucketName, String inputPrefix, String outputPrefix, String keyContaining) {
        findObjectSummaries(bucketName, inputPrefix).stream()
                .filter(objSum -> objSum.getKey().contains(keyContaining))
                .forEach(objSum -> getObjectIfZip(bucketName, objSum).ifPresent(obj -> unzip(obj, outputPrefix)));
    }

    public void unzipObjectsKeyMatching(String bucketName, String inputPrefix, String outputPrefix, String keyMatching) {
        findObjectSummaries(bucketName, inputPrefix).stream()
                .filter(objSum -> objSum.getKey().matches(keyMatching))
                .forEach(objSum -> getObjectIfZip(bucketName, objSum).ifPresent(obj -> unzip(obj, outputPrefix)));
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
                unzipStrategy.unzip(new S3ZipFile(bucketName, outputPrefix, zipInputStream, zipEntry), s3Client);
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
        if (contentTypes == null || contentTypes.isEmpty()) {
            return true;
        }
        return contentTypes.contains(s3Object.getObjectMetadata().getContentType());
    }
}
