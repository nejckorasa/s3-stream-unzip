package com.github.nejckorasa.s3;

import com.github.nejckorasa.s3.unzip.S3UnzipManager;
import com.github.nejckorasa.s3.unzip.strategy.NoSplitUnzipStrategy;
import com.github.nejckorasa.s3.utils.FileUtils;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.skyscreamer.jsonassert.JSONAssert;

import java.nio.file.Files;
import java.nio.file.Path;

import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;

public class UnzipTest {

    public static final String BUCKET_NAME = "test-bucket";
    @RegisterExtension
    private final S3Test s3 = new S3Test();

    @Test
    public void a() {
        FileUtils.generateCsv("file.csv", 10000);
    }

    @Test
    public void unzipsResources() {
        s3.createBuckets(BUCKET_NAME);
        s3.uploadFrom("test-data/raw").to("s3://test-bucket/input");
        s3.uploadFrom("test-data/zip").contentType("application/zip").to("s3://test-bucket/input");
        s3.verifyBucketFileCount("s3://test-bucket", 3);

        new S3UnzipManager(s3.s3Client, new NoSplitUnzipStrategy())
                .unzipObjects(BUCKET_NAME, "input", "output");

        s3.verifyBucketFileCount("s3://test-bucket/input", 3);
        s3.verifyBucketFileCount("s3://test-bucket/output", 2);

        String zippedJson = s3.downloadAsString("s3://test-bucket/output/file.json");
        assertMatchesJson(zippedJson, readFileAsString("test-data/raw/file.json"));

        String zippedCsv = s3.downloadAsString("s3://test-bucket/output/file.csv");
        assertThat(zippedCsv).isEqualTo(readFileAsString("test-data/raw/file.csv"));
    }

    @SneakyThrows
    private void assertMatchesJson(String actual, String expected) {
        JSONAssert.assertEquals(expected, actual, LENIENT);
    }

    @SneakyThrows
    private String readFileAsString(String path) {
        return Files.readString(Path.of(requireNonNull(currentThread().getContextClassLoader().getResource(path)).toURI()));
    }
}
