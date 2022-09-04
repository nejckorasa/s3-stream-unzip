package com.github.nejckorasa.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import io.findify.s3mock.S3Mock;
import lombok.SneakyThrows;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;

public class S3Test implements BeforeEachCallback, AfterEachCallback {

    public static final String US_EAST_1 = "us-east-1";

    S3Mock api;

    int port;

    AmazonS3 s3Client;

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        api = new S3Mock.Builder()
                .withFileBackend("s3-backend")
                .withPort(0)
                .build();

        var serverBinding = api.start();
        port = serverBinding.localAddress().getPort();

        s3Client = AmazonS3ClientBuilder.standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(new EndpointConfiguration("http://localhost:" + port, US_EAST_1))
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        api.shutdown();
    }

    public S3Test createBuckets(String bucketName, String... otherBuckets) {
        s3Client.createBucket(new CreateBucketRequest(bucketName, US_EAST_1));
        for (var otherBucket : otherBuckets) {
            s3Client.createBucket(new CreateBucketRequest(otherBucket, US_EAST_1));
        }
        return this;
    }

    public S3Test verifyBucketFileCount(String s3Path, int expectedCount) {
        var uri = new AmazonS3URI(s3Path);
        var fileCount = s3Client.listObjects(uri.getBucket(), uri.getKey()).getObjectSummaries().size();
        if (fileCount != expectedCount) {
            throw new AssertionError("Expected %d got %d files in %s".formatted(expectedCount, fileCount, uri.getBucket()));
        }
        return this;
    }

    public Upload uploadFrom(String path) {
        return new Upload(path);
    }

    public S3Object download(String s3Path) {
        var uri = new AmazonS3URI(s3Path);
        if (s3Client.doesObjectExist(uri.getBucket(), uri.getKey())) {
            return s3Client.getObject(uri.getBucket(), uri.getKey());
        } else {
            throw new IllegalStateException("Expected s3 object to exist but did not " + s3Path);
        }
    }

    public String downloadAsString(String s3Path) {
        var s3Object = download(s3Path);
        try (var inputStream = s3Object.getObjectContent()) {
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public final class Upload {
        private final Map<String, String> resources;
        private boolean createBuckets = false;
        private String contentType = null;

        @SneakyThrows
        private Upload(String path) {
            resources = new HashMap<>();
            URL resource = currentThread().getContextClassLoader().getResource(path);
            try (var files = Files.list(Paths.get(requireNonNull(resource).toURI()))) {
                files.forEach(p -> resources.put(p.toString(), p.getFileName().toString()));
            }
        }

        public Upload creatingBuckets() {
            createBuckets = true;
            return this;
        }

        public Upload contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public void to(String destination) {
            resources.forEach((path, name) -> {
                try (var inputStream = Files.newInputStream(Path.of(path))) {
                    var uri = new AmazonS3URI(destination);
                    if (createBuckets && !s3Client.doesBucketExistV2(uri.getBucket())) {
                        s3Client.createBucket(uri.getBucket());
                    }
                    var metadata = new ObjectMetadata();
                    if (contentType != null) {
                        metadata.setContentType(contentType);
                    }
                    s3Client.putObject(uri.getBucket(), uri.getKey() + "/" + name, inputStream, metadata);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

    }
}
