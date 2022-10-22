[![Maven Build](https://github.com/nejckorasa/s3-stream-unzip/actions/workflows/maven.yml/badge.svg)](https://github.com/nejckorasa/s3-stream-unzip/actions/workflows/maven.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.nejckorasa/s3-stream-unzip.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.nejckorasa%22%20AND%20a:%22s3-stream-unzip%22)

# s3-stream-unzip

Manages unzipping of data in AWS S3 utilizing stream download and multipart upload. 

Lightweight, only requires AmazonS3 client to run.

Unzipping is achieved without knowing the size beforehand and without keeping it all in memory or writing to disk. That makes it suitable for large data files - it has been used to unzip files of size 100GB+.

Supports different unzip strategies including an option to split zipped files (suitable for larger files), see [SplitTextUnzipStrategy](src/main/java/io/github/nejckorasa/s3/unzip/strategy/SplitTextUnzipStrategy.java).

See [tests](src/test/java/io/github/nejckorasa/s3) (namely [S3UnzipManagerTest](src/test/java/io/github/nejckorasa/s3/S3UnzipManagerTest.java)) for examples on how to
use [S3UnzipManager.java](src/main/java/io/github/nejckorasa/s3/unzip/S3UnzipManager.java) to manage unzipping, some examples:

```java
// initialize AmazonS3 client
AmazonS3 s3CLient = AmazonS3ClientBuilder.standard()
        // customize the client
        .build()

// create UnzipStrategy
var strategy = new NoSplitUnzipStrategy();
var strategy = new SplitTextUnzipStrategy()
        .withHeader(true)
        .withFileBytesLimit(100 * MB);

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

var strategy = new NoSplitUnzipStrategy(config);

// create S3UnzipManager
var um = new S3UnzipManager(s3Client, strategy);
var um = new S3UnzipManager(s3Client, strategy.withContentTypes(List.of("application/zip"));

// unzip options
um.unzipObjects("bucket-name", "input-path", "output-path");
um.unzipObjectsKeyMatching("bucket-name", "input-path", "output-path", ".*\\.zip");
um.unzipObjectsKeyContaining("bucket-name", "input-path", "output-path", "-part-of-object-");
um.unzipObject(s3Object, "output-path");
```
Inspired by: https://medium.com/@pra4mesh/uploading-inputstream-to-aws-s3-using-multipart-upload-java-add81b57964e

## Usage

Available on [Maven Central](https://search.maven.org/artifact/io.github.nejckorasa/s3-stream-unzip/1.0.0/jar).

#### Maven

```xml
<dependency>
    <groupId>io.github.nejckorasa</groupId>
    <artifactId>s3-stream-unzip</artifactId>
    <version>1.0.1</version>
</dependency>
```

#### Gradle

```groovy
compile 'io.github.nejckorasa:s3-stream-unzip:1.0.1'
```

## Unzip strategies

All strategies utilise stream download and multipart upload - unzipping is achieved without keeping all data in memory or writing to disk. 

Refer to tests for usage examples.

### [NoSplitUnzipStrategy](src/main/java/io/github/nejckorasa/s3/unzip/strategy/NoSplitUnzipStrategy.java)
Unzips and uploads a file without splitting (sharding) - it creates a 1:1 mapping between zipped and unzipped file.

- File is read in bytes.

This strategy should ideally be used for smaller files.

### [SplitTextUnzipStrategy](src/main/java/io/github/nejckorasa/s3/unzip/strategy/SplitTextUnzipStrategy.java)
Unzips and uploads a text file with splitting (sharding) - it creates a 1:n mappings between zipped and unzipped files.

- It reads the file as UTF-8 text file split into lines.
- Provides configurable file (shard) size. 
- Can be configured to accommodate files with headers (e.g. csv files). 

This strategy is suitable for larger files as it splits them into smaller, more manageable unzipped files (shards).
