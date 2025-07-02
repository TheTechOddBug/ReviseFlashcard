package com.varshadas.demo.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

public class S3FileHandler {
    private static final Logger logger = LoggerFactory.getLogger(S3FileHandler.class);

    private static final String BUCKET_NAME = System.getenv("S3_BUCKET_NAME");
    private static final String PROCESSED_FLAG_KEY_PREFIX = "processed_flag_";

    public S3FileHandler() {
        if (BUCKET_NAME == null || BUCKET_NAME.isEmpty()) {
            logger.error("S3_BUCKET_NAME environment variable is not set.");
            throw new IllegalStateException("S3_BUCKET_NAME must be defined in the environment variables.");
        }
    }

    public boolean isSlidesPreprocessed(AmazonS3 s3Client, String todayFlagKey) {
        try {
            s3Client.getObjectMetadata(BUCKET_NAME, todayFlagKey);
            logger.info("Slides already preprocessed for today: {}", todayFlagKey);
            return true;
        } catch (Exception e) {
            if (e.getMessage().contains("NoSuchKey")) {
                logger.info("Slides not preprocessed yet for today: {}", todayFlagKey);
                return false;
            } else {
                logger.error("Error checking if slides were preprocessed: {}", e.getMessage());
                throw new RuntimeException("Error checking preprocessed status", e); // rethrowing the exception
            }
        }
    }

    public void markSlidesAsProcessed(AmazonS3 s3Client, String todayFlagKey) {
        try {
            s3Client.putObject(BUCKET_NAME, todayFlagKey, "processed");
            logger.info("Marked slides as processed for today with key: {}", todayFlagKey);
        } catch (Exception e) {
            logger.error("Error marking slides as processed: {}", e.getMessage());
            throw new RuntimeException("Error marking slides as processed", e); // rethrow the exception
        }
    }

    public String getTodayProcessedFlagKey() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("ddMMyy_HHmmss");
        return PROCESSED_FLAG_KEY_PREFIX + dateFormat.format(new Date());
    }

    public void uploadFileToS3(AmazonS3 s3Client, String bucketName, String key, File file) {
        if (file == null || !file.exists()) {
            logger.error("File to upload is either null or does not exist: {}", file);
            throw new IllegalArgumentException("Valid file must be provided.");
        }

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(file.length());

        try (InputStream is = new FileInputStream(file)) {
            s3Client.putObject(new PutObjectRequest(bucketName, key, is, metadata));
            logger.info("Uploaded file to S3: s3://{}/{}", bucketName, key);
        } catch (IOException e) {
            logger.error("Error uploading file to S3: {}", e.getMessage());
            throw new RuntimeException("Error uploading file to S3", e); // rethrow the exception
        }
    }

    public void uploadTextFileToS3(AmazonS3 s3Client, String bucketName, String key, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        try (InputStream inputStream = new ByteArrayInputStream(bytes)) {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(bytes.length);
            s3Client.putObject(bucketName, key, inputStream, metadata);
            logger.info("Uploaded text file to S3: s3://{}/{}", bucketName, key);
        } catch (Exception e) {
            logger.error("Error uploading text to S3: {}", e.getMessage());
            throw new RuntimeException("Error uploading text to S3", e);
        }
    }

    public File downloadFileFromS3(AmazonS3 s3Client, String fileName) {
        try {
            File tempFile = File.createTempFile(fileName, ".tmp");
            s3Client.getObject(new GetObjectRequest(BUCKET_NAME, fileName), tempFile);
            logger.info("File downloaded from S3 to {}", tempFile.getAbsolutePath());
            return tempFile;
        } catch (IOException e) {
            logger.error("Error downloading file from S3: {}", e.getMessage());
            throw new RuntimeException("Error downloading file from S3", e); // rethrowing the exception
        }
    }
}
