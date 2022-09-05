[![Maven Build](https://github.com/nejckorasa/s3-stream-unzip/actions/workflows/maven.yml/badge.svg)](https://github.com/nejckorasa/s3-stream-unzip/actions/workflows/maven.yml)

# s3-stream-unzip

Manages unzipping of data in AWS S3 utilizing stream download and multipart upload. Unzipping is achieved without knowing the size beforehand and without keeping it all in memory or writing to disk.

See [tests](src/test/java/com/github/nejckorasa/s3) (namely [S3UnzipManagerTest](src/test/java/com/github/nejckorasa/s3/S3UnzipManagerTest.java)) for examples on how to
use [S3UnzipManager.java](src/main/java/com/github/nejckorasa/s3/unzip/S3UnzipManager.java) to manage unzipping, some examples:

```java
// initialize AmazonS3 client
AmazonS3 s3CLient = AmazonS3ClientBuilder.standard()
        // customize the client
        .build()

// create UnzipStrategy
UnzipStrategy strategy = new NoSplitUnzipStrategy();

// or create UnzipStrategy with additional config
var config = new S3MultipartUpload.Config()
        .withThreadCount(5)
        .withQueueSize(5)
        .withAwaitTerminationTimeSeconds(2)
        .withCannedAcl(CannedAccessControlList.BucketOwnerFullControl)
        .withUploadPartBytesLimit(20 * MB)
        .withCustomizeInitiateUploadRequest(request -> {
            // customize request
            return request;
        });

UnzipStrategy strategy = new NoSplitUnzipStrategy(config);

// create S3UnzipManager
var um = new S3UnzipManager(s3Client, strategy);
var um = new S3UnzipManager(s3Client, strategy.withContentTypes(List.of("application/zip"));

// unzip options
um.unzipObjects("bucket-name", "input-path", "output-path");
um.unzipObjectsKeyMatching("bucket-name", "input-path", "output-path", ".*\\.zip");
um.unzipObjectsKeyContaining("bucket-name", "input-path", "output-path", "-part-of-object-");
um.unzipObject(s3Object, "output-path");
```
