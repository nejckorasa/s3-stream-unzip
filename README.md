[![Maven Build](https://github.com/nejckorasa/s3-stream-unzip/actions/workflows/maven.yml/badge.svg)](https://github.com/nejckorasa/s3-stream-unzip/actions/workflows/maven.yml)

# s3-stream-unzip

Manages unzipping of data in AWS S3 without knowing the size beforehand and without keeping it all in memory or writing to disk.

See [UnzipTest.java](src/test/java/com/github/nejckorasa/s3/UnzipTest.java) for examples on how to
use [S3UnzipManager.java](src/main/java/com/github/nejckorasa/s3/unzip/S3UnzipManager.java) to manage unzipping, for example:

```java
// create UnzipStrategy
UnzipStrategy strategy = new NoSplitUnzipStrategy();

// create UnzipStrategy with additional config
var config = new S3MultipartUpload.Config()
        .withThreadCount(5)
        .withQueueSize(5)
        .withAwaitTerminationTimeSeconds(2)
        .withCannedAcl(CannedAccessControlList.BucketOwnerFullControl)
        .withUploadPartBytesLimit(20 * MB)
        .withCustomizeInitiateUploadRequest(request->{
            // customize request
            return request;
        });

UnzipStrategy strategy = new NoSplitUnzipStrategy(config);

// create S3UnzipManager
var um = new S3UnzipManager(s3Client,strategy);

// unzip
um.unzipObjects("bucket-name", "input-path", "output-path");
um.unzipObjectsKeyMatching("bucket-name", "input-path", "output-path", "*.zip");
```

## TODOs

### Add SplitUnzipStrategy

Add a strategy that allows larger files to be split to into multiple parts/files that are uploaded to S3.

### Improve tests w/ unzip of larger files

Code has been tested manually with real S3 and is working, but is still missing tests. At the moment tests rely on S3Mock with in-memory
backend. Test files are too small to trigger multipart upload with more than 1 part.

- Improve tests with unzipping of larger files and actually test the multipart upload. Explore file-backend for S3Mock to generate larger
  test files dynamically.
- Improve overall test coverage

### Publish the code to maven central if it proves to be useful
