package com.github.nejckorasa.s3;

import com.github.nejckorasa.s3.unzip.S3UnzipManager;
import com.github.nejckorasa.s3.unzip.strategy.NoSplitUnzipStrategy;
import com.github.nejckorasa.s3.upload.S3MultipartUpload;
import org.json.JSONException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.skyscreamer.jsonassert.JSONAssert;

import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;

public class UnzipTest {

    public static final String BUCKET_NAME = "test-bucket";
    @RegisterExtension
    private final S3Test s3 = new S3Test();

    @Test
    public void unzipsAResource() throws JSONException {
        s3.uploadFrom("test-data").creatingBuckets().to("s3://test-bucket/zipped");
        s3.verifyBucketFileCount("s3://test-bucket", 1);

        var unzipStrategy = new NoSplitUnzipStrategy(new S3MultipartUpload.Config().withContentType("application/json"));
        var unzipManager = new S3UnzipManager(s3.s3Client, unzipStrategy);

        unzipManager.unzipObjects(BUCKET_NAME, "zipped", "unzipped");

        s3.verifyBucketFileCount("s3://test-bucket/zipped", 1);
        s3.verifyBucketFileCount("s3://test-bucket/unzipped", 1);

        String actualObject = s3.downloadAsString("s3://test-bucket/unzipped/test-file.json");
        String expectedObject = """
                {
                  "name": "Test Json Object"
                }
                """;

        JSONAssert.assertEquals(expectedObject, actualObject, LENIENT);
    }
}
