package io.github.nejckorasa.s3;

import io.github.nejckorasa.s3.unzip.S3UnzipManager;
import io.github.nejckorasa.s3.unzip.strategy.NoSplitUnzipStrategy;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.List;

import static io.github.nejckorasa.s3.utils.FileUtils.readFileAsString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;

public class S3UnzipManagerTest {

    public static final String BUCKET_NAME = "test-bucket";

    @RegisterExtension
    private final S3Test s3 = new S3Test().withDefaultBucket(BUCKET_NAME);

    @BeforeEach
    public void beforeEach() {
        s3.uploadFrom("test-data/raw").to("s3://test-bucket/input");
        s3.uploadFrom("test-data/zip/subfolder")
                .contentType("application/zip")
                .to("s3://test-bucket/input/subfolder");

        s3.uploadFrom("test-data/zip/flat")
                .contentType("application/zip")
                .to("s3://test-bucket/input/flat");

        s3.verifyBucketFileCount("s3://test-bucket", 4);
    }

    @Test
    public void unzipsObjectsInsideZipArchive() {
        var um = new S3UnzipManager(s3.s3Client, new NoSplitUnzipStrategy());
        um.unzipObjects(BUCKET_NAME, "input", "output");

        s3.verifyBucketFileCount("s3://test-bucket/input", 4);
        s3.verifyContainsFiles("s3://test-bucket/output",
                "output/file.json",
                "output/file.csv");

        assertMatchesJson(
                s3.downloadAsString("s3://test-bucket/output/file.json"),
                readFileAsString("test-data/raw/file.json"));

        assertThat(s3.downloadAsString("s3://test-bucket/output/file.csv"))
                .isEqualTo(readFileAsString("test-data/raw/file.csv"));
    }

    @Test
    public void unzipsObjectsInSubfolderOfZipArchive() {
        var um = new S3UnzipManager(s3.s3Client, new NoSplitUnzipStrategy());
        um.unzipObjects(BUCKET_NAME, "input/subfolder", "output");

        s3.verifyBucketFileCount("s3://test-bucket/input", 4);
        s3.verifyContainsFiles("s3://test-bucket/output",
                "output/Archive/file.json",
                "output/Archive/folder/file.csv",
                "output/Archive/folder/subfolder/another_file.csv");

        assertMatchesJson(
                s3.downloadAsString("s3://test-bucket/output/Archive/file.json"),
                readFileAsString("test-data/raw/file.json"));

        assertThat(s3.downloadAsString("s3://test-bucket/output/Archive/folder/file.csv"))
                .isEqualTo(readFileAsString("test-data/raw/file.csv"));

        assertThat(s3.downloadAsString("s3://test-bucket/output/Archive/folder/subfolder/another_file.csv"))
                .isEqualTo(readFileAsString("test-data/raw/file.csv"));
    }

    @Test
    public void unzipsAnObject() {
        var um = new S3UnzipManager(s3.s3Client, new NoSplitUnzipStrategy());

        um.unzipObject(s3.download("s3://test-bucket/input/flat/Archive.zip"), "output");
        s3.verifyBucketFileCount("s3://test-bucket/output", 2);

        assertMatchesJson(s3.downloadAsString("s3://test-bucket/output/file.json"), readFileAsString("test-data/raw/file.json"));
        assertThat(s3.downloadAsString("s3://test-bucket/output/file.csv")).isEqualTo(readFileAsString("test-data/raw/file.csv"));
    }

    @Test
    public void unzipsObjectsWithContentType() {
        var unzipStrategy = new NoSplitUnzipStrategy();

        new S3UnzipManager(s3.s3Client, unzipStrategy)
                .withContentTypes(List.of("application/zip"))
                .unzipObjects(BUCKET_NAME, "input", "output");

        s3.verifyContainsFiles("s3://test-bucket/output",
                "output/file.json",
                "output/file.csv",
                "output/Archive/file.json",
                "output/Archive/folder/file.csv",
                "output/Archive/folder/subfolder/another_file.csv");

        new S3UnzipManager(s3.s3Client, unzipStrategy)
                .withContentTypes(List.of("unmatched-content-type"))
                .unzipObjects(BUCKET_NAME, "input", "output-2");

        s3.verifyBucketFileCount("s3://test-bucket/output-2", 0);
    }

    @Test
    public void unzipsObjectsMatchingKey() {
        var um = new S3UnzipManager(s3.s3Client, new NoSplitUnzipStrategy());

        um.unzipObjectsKeyMatching(BUCKET_NAME, "input", "output", ".*\\.zip");
        s3.verifyContainsFiles("s3://test-bucket/output",
                "output/file.json",
                "output/file.csv",
                "output/Archive/file.json",
                "output/Archive/folder/file.csv",
                "output/Archive/folder/subfolder/another_file.csv");

        um.unzipObjectsKeyMatching(BUCKET_NAME, "input", "output-2", ".*\\.unmatched");
        s3.verifyBucketFileCount("s3://test-bucket/output-2", 0);
    }

    @Test
    public void unzipsObjectsContainingKey() {
        var um = new S3UnzipManager(s3.s3Client, new NoSplitUnzipStrategy());

        um.unzipObjectsKeyContaining(BUCKET_NAME, "input", "output", ".zip");
        s3.verifyContainsFiles("s3://test-bucket/output",
                "output/file.json",
                "output/file.csv",
                "output/Archive/file.json",
                "output/Archive/folder/file.csv",
                "output/Archive/folder/subfolder/another_file.csv");

        um.unzipObjectsKeyContaining(BUCKET_NAME, "input", "output-2", ".unmatched");
        s3.verifyBucketFileCount("s3://test-bucket/output-2", 0);
    }

    @SneakyThrows
    private void assertMatchesJson(String actual, String expected) {
        JSONAssert.assertEquals(expected, actual, LENIENT);
    }
}
