package io.github.nejckorasa.s3.unzip.strategy;

import com.amazonaws.services.s3.AmazonS3;
import io.github.nejckorasa.s3.unzip.S3UnzipException;
import io.github.nejckorasa.s3.unzip.S3ZipFile;
import io.github.nejckorasa.s3.upload.S3MultipartUpload;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.util.Scanner;

import static com.amazonaws.services.s3.internal.Constants.MB;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Unzips and uploads a text file with splitting (sharding) - it creates a 1:n mappings between zipped and unzipped files.
 *
 * <p> It reads the file as UTF-8 text file split into lines.
 * <p> Set {@link #header} to 'true' if zipped file contains a header that needs to be included with every split file/shard (e.g. csv files). Defaults to false.
 * <p> This strategy is suitable for larger files as it splits them into smaller, more manageable unzipped files (shards).
 *
 * <p> Utilizes multipart upload - unzipping is achieved without keeping all data in memory or writing to disk.
 */
@Slf4j
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SplitTextUnzipStrategy implements UnzipStrategy {

    public static final String LINE_BREAK = "\n";

    /**
     * S3 multipart upload part limit in bytes.
     *
     * @see S3MultipartUpload
     */
    @NonNull
    @With
    private int uploadPartBytesLimit = 20 * MB;

    /**
     * Add a header line to all files, i.e. first line of the source zip entry will be replicated to all output files
     */
    @With
    private boolean header = false;

    /**
     * File (shard) size limit, i.e. 100 MB will split the source zip entry into files with size limit of 100 MB
     */
    @NonNull
    @With
    private long fileBytesLimit = 100 * MB;

    /**
     * Configuration for S3 multipart upload. Configures {@link S3MultipartUpload},
     */
    @NonNull
    private S3MultipartUpload.Config config = S3MultipartUpload.Config.DEFAULT;

    /**
     * Creates SplitTextUnzipStrategy with provided configuration for {@link S3MultipartUpload}
     *
     * @param config Multipart upload configuration for {@link S3MultipartUpload}
     */
    public SplitTextUnzipStrategy(@NonNull S3MultipartUpload.Config config) {
        this.config = config;
    }

    @Override
    public void unzip(S3ZipFile zipFile, AmazonS3 s3Client) {
        String filename = zipFile.filename();
        long compressedSize = zipFile.compressedSize();
        long size = zipFile.size();
        String key = zipFile.key();

        log.info("Unzipping {}, compressed: {} bytes, extracted: {} bytes to {}", filename, compressedSize, size, key);

        int fileNumber = 1;
        Scanner scanner = new Scanner(zipFile.getInputStream(), UTF_8);
        var s3MultipartUpload = initializeS3MultipartUpload(s3Client, zipFile, fileNumber);

        try {
            var outputStream = new ByteArrayOutputStream();

            long allBytesRead = 0;
            long uploadPartBytes = 0;
            long fileBytes = 0;

            String headerLine = null;

            long partNumber = 0;
            boolean newFile = false;

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine() + LINE_BREAK;

                if (header && headerLine == null) {
                    headerLine = line;
                }

                long bytesRead = 0;

                // write header line if new file
                if (header && newFile) {
                    bytesRead += writeLine(headerLine, outputStream);
                    newFile = false;
                }

                // write line
                bytesRead += writeLine(line, outputStream);
                fileBytes += bytesRead;
                allBytesRead += bytesRead;

                if (uploadPartBytes < uploadPartBytesLimit) {
                    uploadPartBytes += bytesRead;
                    continue;
                }

                // upload new part
                partNumber += 1;

                // have reached file bytes limit
                if (fileBytes > fileBytesLimit) {
                    log.debug("Uploading final part [{}] for file: {} and shard file number: {} - Read {} bytes out of {} bytes", partNumber, filename, fileNumber, allBytesRead, size);

                    // finalize upload with current file
                    s3MultipartUpload.uploadFinalPart(outputStream.toByteArray());
                    partNumber = 1;
                    fileNumber += 1;
                    fileBytes = 0;
                    newFile = true;

                    log.info("Unzipped and uploaded file: {} shard file number {} in {} parts", filename, fileNumber, partNumber);

                    // initialize new multipart upload
                    s3MultipartUpload = initializeS3MultipartUpload(s3Client, zipFile, fileNumber);
                } else {
                    log.debug("Uploading part [{}] for file: {} and shard file number: {} - Read {} bytes out of {} bytes", partNumber, filename, fileNumber, allBytesRead, size);
                    s3MultipartUpload.uploadPart(outputStream.toByteArray());
                }

                outputStream.reset();
                uploadPartBytes = 0;
            }

            // upload remaining part of output stream as final part
            s3MultipartUpload.uploadFinalPart(outputStream.toByteArray());
            log.info("Unzipped and uploaded file: {} sharded into {} files", filename, fileNumber);

        } catch (Throwable t) {
            s3MultipartUpload.abort();
            throw new S3UnzipException("Failed to unzip " + filename, t);
        }
    }

    private S3MultipartUpload initializeS3MultipartUpload(AmazonS3 s3Client, S3ZipFile s3ZipFile, int fileNumber) {
        String filenameWithNumber = fileNumber + "-" + s3ZipFile.filename();
        log.debug("Initializing upload for file: {}", filenameWithNumber);

        String key = s3ZipFile.getOutputPrefix() + filenameWithNumber;
        var multipartUpload = new S3MultipartUpload(s3ZipFile.getBucketName(), key, s3Client, config);
        multipartUpload.initialize();

        return multipartUpload;
    }

    private long writeLine(String line, ByteArrayOutputStream outputStream) {
        byte[] bytes = line.getBytes(UTF_8);
        int bytesRead = bytes.length;
        outputStream.write(bytes, 0, bytesRead);
        return bytesRead;
    }
}
