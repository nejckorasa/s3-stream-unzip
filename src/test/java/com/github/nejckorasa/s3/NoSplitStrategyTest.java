package com.github.nejckorasa.s3;

import com.github.nejckorasa.s3.unzip.S3UnzipManager;
import com.github.nejckorasa.s3.unzip.strategy.NoSplitUnzipStrategy;
import com.github.nejckorasa.s3.utils.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

public class NoSplitStrategyTest {

    public static final String BUCKET_NAME = "test-bucket";
    public static final String S3_BACKEND_PATH = "tmp/s3-backend";

    @RegisterExtension
    private final S3Test s3 = new S3Test().withLocalFileBackend(S3_BACKEND_PATH);

    @Test
    public void unzipsLargeCsvResource() {
        s3.createBuckets(BUCKET_NAME);
        var csvBytes = FileUtils.generateZippedCsv(
                Paths.get(S3_BACKEND_PATH, BUCKET_NAME, "input"),
                "test.csv",
                2_000_000);

        s3.verifyBucketFileCount("s3://test-bucket", 1);

        var um = new S3UnzipManager(s3.s3Client, new NoSplitUnzipStrategy());
        um.unzipObjects(BUCKET_NAME, "input", "output");

        s3.verifyBucketFileCount("s3://test-bucket/input", 1);
        s3.verifyBucketFileCount("s3://test-bucket/output", 1);

        assertThat(s3.downloadAsBytes("s3://test-bucket/output/test.csv")).isEqualTo(csvBytes);
    }
}
