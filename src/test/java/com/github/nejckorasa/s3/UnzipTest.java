package com.github.nejckorasa.s3;

import com.github.nejckorasa.s3.unzip.S3UnzipManager;
import com.github.nejckorasa.s3.unzip.strategy.NoSplitUnzipStrategy;
import com.github.nejckorasa.s3.upload.S3MultipartUpload;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class UnzipTest {

    public static final String BUCKET_NAME = "test-bucket";
    @RegisterExtension
    private final S3Test s3 = new S3Test();

    @Test
    public void unzipsAResource() {
        s3.uploadFrom("test-data").creatingBuckets().to("s3://test-bucket/zipped");
        s3.verifyBucketFileCount("s3://test-bucket", 1);

        var unzipManager = new S3UnzipManager(
                s3.s3Client,
                new NoSplitUnzipStrategy(new S3MultipartUpload.Config().withContentType("application/json")));

        unzipManager.unzipObjects(BUCKET_NAME, "zipped", "unzipped");

        s3.verifyBucketFileCount("s3://test-bucket/zipped", 1);
        s3.verifyBucketFileCount("s3://test-bucket/unzipped", 1);
    }
}
