package io.github.nejckorasa.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.findify.s3mock.S3Mock;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Thread.currentThread;
import static java.util.Comparator.reverseOrder;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class S3Test implements BeforeEachCallback, AfterEachCallback {

    public static final String US_EAST_1 = "us-east-1";

    String localFileBackendPath;

    String defaultBucketName;

    S3Mock api;

    int port;

    AmazonS3 s3Client;

    public S3Test withLocalFileBackend(String path) {
        localFileBackendPath = path;
        return this;
    }

    public S3Test withDefaultBucket(String bucketName) {
        defaultBucketName = bucketName;
        return this;
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        var apiBuilder = new S3Mock.Builder().withPort(0);
        if (localFileBackendPath != null) {
            apiBuilder.withFileBackend(localFileBackendPath);
        } else {
            apiBuilder.withInMemoryBackend();
        }
        api = apiBuilder.build();

        var serverBinding = api.start();
        port = serverBinding.localAddress().getPort();

        s3Client = AmazonS3ClientBuilder.standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(new EndpointConfiguration("http://localhost:" + port, US_EAST_1))
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();

        if (defaultBucketName != null) {
            createBuckets(defaultBucketName);
        }
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        api.shutdown();
        if (localFileBackendPath != null) {
            try (var files = Files.walk(Path.of(localFileBackendPath))) {
                files.sorted(reverseOrder()).map(Path::toFile).forEach(File::delete);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
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
        var objectSummaries = s3Client.listObjects(uri.getBucket(), uri.getKey()).getObjectSummaries();
        log.debug("Object summaries {}", objectSummaries.stream().map(S3ObjectSummary::getKey).collect(joining(",", "[", "]")));
        var fileCount = objectSummaries.size();
        if (fileCount != expectedCount) {
            throw new AssertionError(String.format("Expected %d got %d files in %s", expectedCount, fileCount, uri.getBucket()));
        }
        return this;
    }

    public S3Test verifyContainsFiles(String s3Path, String... expectedObjectKeys) {
        var uri = new AmazonS3URI(s3Path);
        var objectSummaries = s3Client.listObjects(uri.getBucket(), uri.getKey()).getObjectSummaries();
        List<String> objectKeys = objectSummaries.stream().map(S3ObjectSummary::getKey).collect(toList());
        assertThat(objectKeys).containsAll(Arrays.asList(expectedObjectKeys));
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

    public byte[] downloadAsBytes(String s3Path) {
        var s3Object = download(s3Path);
        try (var inputStream = s3Object.getObjectContent()) {
            return inputStream.readAllBytes();
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
