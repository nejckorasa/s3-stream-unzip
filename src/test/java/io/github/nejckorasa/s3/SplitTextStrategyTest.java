package io.github.nejckorasa.s3;

import io.github.nejckorasa.s3.unzip.S3UnzipManager;
import io.github.nejckorasa.s3.unzip.strategy.SplitTextUnzipStrategy;
import io.github.nejckorasa.s3.utils.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayInputStream;
import java.nio.file.Paths;
import java.util.List;

import static com.amazonaws.services.s3.internal.Constants.MB;
import static io.github.nejckorasa.s3.utils.FileUtils.readTxt;
import static org.assertj.core.api.Assertions.assertThat;

public class SplitTextStrategyTest {

    public static final String BUCKET_NAME = "test-bucket";
    public static final String S3_BACKEND_PATH = "tmp/s3-backend";

    @RegisterExtension
    private final S3Test s3 = new S3Test()
            .withLocalFileBackend(S3_BACKEND_PATH)
            .withDefaultBucket(BUCKET_NAME);

    @Test
    public void unzipsAndSplitsLargeCsvObjectWithHeader() {
        var csvBytes = FileUtils.generateZippedTxtFile(
                Paths.get(S3_BACKEND_PATH, BUCKET_NAME, "input"),
                "test.csv",
                2_000_000);

        s3.verifyBucketFileCount("s3://test-bucket", 1);

        var strategy = new SplitTextUnzipStrategy()
                .withFileBytesLimit(10 * MB)
                .withHeader(true);

        var um = new S3UnzipManager(s3.s3Client, strategy);
        um.unzipObjects(BUCKET_NAME, "input", "output");

        s3.verifyBucketFileCount("s3://test-bucket/input", 1);
        s3.verifyBucketFileCount("s3://test-bucket/output", 6);

        assertMatchesDataSplitInObjects(csvBytes, true, List.of(
                "s3://test-bucket/output/1-test.csv",
                "s3://test-bucket/output/2-test.csv",
                "s3://test-bucket/output/3-test.csv",
                "s3://test-bucket/output/4-test.csv",
                "s3://test-bucket/output/5-test.csv",
                "s3://test-bucket/output/6-test.csv"
        ));
    }

    @Test
    public void unzipsAndSplitsLargeTxtObjectWithoutHeader() {
        var txtBytes = FileUtils.generateZippedTxtFile(
                Paths.get(S3_BACKEND_PATH, BUCKET_NAME, "input"),
                "test.txt",
                2_000_000);

        s3.verifyBucketFileCount("s3://test-bucket", 1);

        var strategy = new SplitTextUnzipStrategy()
                .withFileBytesLimit(10 * MB)
                .withHeader(false);

        var um = new S3UnzipManager(s3.s3Client, strategy);
        um.unzipObjects(BUCKET_NAME, "input", "output");

        s3.verifyBucketFileCount("s3://test-bucket/input", 1);
        s3.verifyBucketFileCount("s3://test-bucket/output", 6);

        assertMatchesDataSplitInObjects(txtBytes, false, List.of(
                "s3://test-bucket/output/1-test.txt",
                "s3://test-bucket/output/2-test.txt",
                "s3://test-bucket/output/3-test.txt",
                "s3://test-bucket/output/4-test.txt",
                "s3://test-bucket/output/5-test.txt",
                "s3://test-bucket/output/6-test.txt"
        ));
    }

    private void assertMatchesDataSplitInObjects(byte[] data, boolean withHeader, List<String> objectPaths) {
        var expected = readTxt(new ByteArrayInputStream(data));

        var linesIterator = expected.getLines().iterator();
        if (withHeader) {
            linesIterator.next();
        }

        objectPaths.forEach(objPath -> {
            var objectData = readTxt(s3.download(objPath).getObjectContent());

            assertThat(objectData.hasLines()).isTrue();

            if (withHeader) {
                assertThat(objectData.header()).isEqualTo(expected.header());
                objectData.streamLinesWithoutHeader().forEach(line -> assertThat(line).isEqualTo(linesIterator.next()));
            } else {
                objectData.streamLines().forEach(line -> assertThat(line).isEqualTo(linesIterator.next()));
            }
        });

        assertThat(linesIterator.hasNext()).isFalse();
    }
}
