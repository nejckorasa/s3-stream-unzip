package com.github.nejckorasa.s3;

import com.github.nejckorasa.s3.unzip.S3UnzipManager;
import com.github.nejckorasa.s3.unzip.strategy.NoSplitUnzipStrategy;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.List;

import static com.github.nejckorasa.s3.utils.FileUtils.readFileAsString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;

public class S3UnzipManagerTest {

    public static final String BUCKET_NAME = "test-bucket";

    @RegisterExtension
    private final S3Test s3 = new S3Test();

    @Test
    public void unzipsResources() {
        s3.createBuckets(BUCKET_NAME);
        s3.uploadFrom("test-data/raw").to("s3://test-bucket/input");
        s3.uploadFrom("test-data/zip").contentType("application/zip").to("s3://test-bucket/input");
        s3.verifyBucketFileCount("s3://test-bucket", 3);

        var um = new S3UnzipManager(s3.s3Client, new NoSplitUnzipStrategy());
        um.unzipObjects(BUCKET_NAME, "input", "output");

        s3.verifyBucketFileCount("s3://test-bucket/input", 3);
        s3.verifyBucketFileCount("s3://test-bucket/output", 2);

        assertMatchesJson(s3.downloadAsString("s3://test-bucket/output/file.json"), readFileAsString("test-data/raw/file.json"));
        assertThat(s3.downloadAsString("s3://test-bucket/output/file.csv")).isEqualTo(readFileAsString("test-data/raw/file.csv"));
    }

    @Test
    public void unzipsResourcesWithContentType() {
        s3.createBuckets(BUCKET_NAME);
        s3.uploadFrom("test-data/raw").to("s3://test-bucket/input");
        s3.uploadFrom("test-data/zip").contentType("application/zip").to("s3://test-bucket/input");
        s3.verifyBucketFileCount("s3://test-bucket", 3);

        var unzipStrategy = new NoSplitUnzipStrategy();

        new S3UnzipManager(s3.s3Client, unzipStrategy)
                .withContentTypes(List.of("application/zip"))
                .unzipObjects(BUCKET_NAME, "input", "output");
        s3.verifyBucketFileCount("s3://test-bucket/output", 2);

        new S3UnzipManager(s3.s3Client, unzipStrategy)
                .withContentTypes(List.of("unmatched-content-type"))
                .unzipObjects(BUCKET_NAME, "input", "output-2");
        s3.verifyBucketFileCount("s3://test-bucket/output-2", 0);
    }

    @Test
    public void unzipsResourcesMatchingKey() {
        s3.createBuckets(BUCKET_NAME);
        s3.uploadFrom("test-data/raw").to("s3://test-bucket/input");
        s3.uploadFrom("test-data/zip").contentType("application/zip").to("s3://test-bucket/input");
        s3.verifyBucketFileCount("s3://test-bucket", 3);

        var um = new S3UnzipManager(s3.s3Client, new NoSplitUnzipStrategy());

        um.unzipObjectsKeyMatching(BUCKET_NAME, "input", "output", ".*\\.zip");
        s3.verifyBucketFileCount("s3://test-bucket/output", 2);

        um.unzipObjectsKeyMatching(BUCKET_NAME, "input", "output-2", ".*\\.unmatched");
        s3.verifyBucketFileCount("s3://test-bucket/output-2", 0);
    }

    @Test
    public void unzipsResourcesContainingKey() {
        s3.createBuckets(BUCKET_NAME);
        s3.uploadFrom("test-data/raw").to("s3://test-bucket/input");
        s3.uploadFrom("test-data/zip").contentType("application/zip").to("s3://test-bucket/input");
        s3.verifyBucketFileCount("s3://test-bucket", 3);

        var um = new S3UnzipManager(s3.s3Client, new NoSplitUnzipStrategy());

        um.unzipObjectsKeyContaining(BUCKET_NAME, "input", "output", ".zip");
        s3.verifyBucketFileCount("s3://test-bucket/output", 2);

        um.unzipObjectsKeyContaining(BUCKET_NAME, "input", "output-2", ".unmatched");
        s3.verifyBucketFileCount("s3://test-bucket/output-2", 0);
    }

    @SneakyThrows
    private void assertMatchesJson(String actual, String expected) {
        JSONAssert.assertEquals(expected, actual, LENIENT);
    }
}
