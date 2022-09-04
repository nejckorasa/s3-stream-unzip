package com.github.nejckorasa.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.github.nejckorasa.s3.upload.exception.S3MultipartUploadException;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.With;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.amazonaws.services.s3.internal.Constants.MB;
import static com.github.nejckorasa.s3.Assertions.assertNotBlank;
import static com.github.nejckorasa.s3.Assertions.assertOrThrow;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class S3MultipartUpload {

    public static final int MAX_UPLOAD_NUMBER = 10_000;
    public static final int MIN_UPLOAD_PART_BYTES_SIZE = 5 * MB;
    private final AtomicInteger uploadPartNumber = new AtomicInteger(0);
    private final Config config;
    private final String bucketName;
    private final String key;
    private final ExecutorService executorService;
    private final AmazonS3 s3Client;
    private String uploadId;
    private volatile boolean isAborting = false;
    private final List<Future<PartETag>> partETagFutures = new ArrayList<>();

    @NoArgsConstructor
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Config {

        public static final Config DEFAULT = new Config();

        @With
        private int awaitTerminationTimeSeconds = 2;

        @With
        private int threadCount = 4;

        @With
        private int queueSize = 4;

        @With
        private int uploadPartBytesLimit = 20 * MB;

        @With
        private CannedAccessControlList cannedAcl;

        @With
        private String contentType;

        @With
        private Function<InitiateMultipartUploadRequest, InitiateMultipartUploadRequest> customizeInitiateUploadRequest;
    }

    public S3MultipartUpload(String bucketName, String key, AmazonS3 s3Client, Config config) {
        var threadPoolExecutor = new ThreadPoolExecutor(
                config.threadCount, config.threadCount,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.queueSize));

        threadPoolExecutor.setRejectedExecutionHandler((r, executor) -> {
            try {
                if (!executor.isShutdown()) {
                    executor.getQueue().put(r);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException("Executor was interrupted while the task was waiting to be put on the work queue", e);
            }
        });

        assertOrThrow(
                () -> config.uploadPartBytesLimit < MIN_UPLOAD_PART_BYTES_SIZE,
                "Part size cannot be smaller than " + MIN_UPLOAD_PART_BYTES_SIZE);

        this.config = config;
        this.executorService = threadPoolExecutor;
        this.bucketName = bucketName;
        this.key = key;
        this.s3Client = s3Client;
    }

    public void initializeUpload() {
        var initRequest = new InitiateMultipartUploadRequest(bucketName, key);
        initRequest.setTagging(new ObjectTagging(new ArrayList<>()));

        var metadata = new ObjectMetadata();
        metadata.setContentType(config.contentType);
        initRequest.setObjectMetadata(metadata);

        if (config.cannedAcl != null) {
            initRequest.withCannedACL(config.cannedAcl);
        }

        if (config.customizeInitiateUploadRequest != null) {
            initRequest = config.customizeInitiateUploadRequest.apply(initRequest);
        }

        try {
            uploadId = s3Client.initiateMultipartUpload(initRequest).getUploadId();
        } catch (Throwable t) {
            log.error("Failed initialising multipart upload with uploadId {}", uploadId);
            throw abort(t);
        }
    }

    public void uploadPart(ByteArrayInputStream inputStream) {
        submitUploadPart(inputStream, false);
    }

    public void uploadFinalPart(ByteArrayInputStream inputStream) {
        try {
            submitUploadPart(inputStream, true);
            var partETags = waitForAllUploadParts();
            s3Client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags));
        } catch (Throwable t) {
            log.error("Failed to upload final part");
            throw abort(t);
        } finally {
            shutdownAndAwaitTermination();
        }
    }

    private void submitUploadPart(ByteArrayInputStream inputStream, boolean finalPart) {
        assertNotBlank(uploadId, "uploadId has not been set");
        assertNotBlank(bucketName, "bucketName has not been set");
        assertNotBlank(key, "key has not been set");

        submitTask(() -> {
            int partNumber = incrementUploadNumber();
            int partSize = inputStream.available();

            var uploadPartRequest = new UploadPartRequest()
                    .withBucketName(bucketName)
                    .withKey(key)
                    .withUploadId(uploadId)
                    .withPartNumber(partNumber)
                    .withPartSize(partSize)
                    .withInputStream(inputStream);

            if (finalPart) {
                uploadPartRequest.withLastPart(true);
            }

            try {
                log.debug("Submitting partNumber {}, with partSize {}", partNumber, partSize);
                var uploadPartResult = s3Client.uploadPart(uploadPartRequest);
                log.debug("Submitted partNumber {}", partNumber);
                return uploadPartResult.getPartETag();
            } catch (Throwable t) {
                throw abort(t);
            }
        });
    }

    private void submitTask(Callable<PartETag> task) {
        var partETagFuture = executorService.submit(task);
        partETagFutures.add(partETagFuture);
    }

    private List<PartETag> waitForAllUploadParts() throws InterruptedException, ExecutionException {
        List<PartETag> partETags = new ArrayList<>();
        for (var partETagFuture : partETagFutures) {
            partETags.add(partETagFuture.get());
        }
        return partETags;
    }

    private void shutdownAndAwaitTermination() {
        log.debug("Shutting down executor service for uploadId {}", uploadId);
        executorService.shutdown();
        try {
            executorService.awaitTermination(config.awaitTerminationTimeSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted while awaiting executor service shutdown");
            Thread.currentThread().interrupt();
        }
        executorService.shutdownNow();
    }

    private int incrementUploadNumber() {
        int uploadNumber = uploadPartNumber.incrementAndGet();
        if (uploadNumber > MAX_UPLOAD_NUMBER) {
            throw new IllegalStateException("Upload part number cannot exceed " + MAX_UPLOAD_NUMBER);
        }
        return uploadNumber;
    }

    public RuntimeException abort(Throwable t) {
        if (!isAborting) {
            log.error("Aborting {} due to error: {}", this, t);
        }

        abort();

        if (t instanceof Error) {
            throw (Error) t;
        } else if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            throw new S3MultipartUploadException();
        } else {
            throw new S3MultipartUploadException("S3MultipartUpload aborted", t);
        }
    }

    public void abort() {
        synchronized (this) {
            if (isAborting) {
                return;
            }
            isAborting = true;
            if (uploadId != null) {
                log.debug("{}: Aborting", this);
                s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
                log.info("{}: Aborted", this);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("[S3MultipartUpload uploading to %s/%s, with uploadId %s", bucketName, key, uploadId);
    }
}
