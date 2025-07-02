package com.varshadas.demo.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;


public class S3CleanupService {

        private static final Logger logger = LoggerFactory.getLogger(S3CleanupService.class);
        private static final String BUCKET_NAME = System.getenv("S3_BUCKET_NAME");
        private static final String PROCESSED_FLAG_KEY_PREFIX = "processed_flag_";


    public void cleanupPreviousDayProcessedSlides(AmazonS3 s3Client) {
            logger.info("Cleaning up previous day's slides");
            String yesterdayProcessedFlagKey = getYesterdayProcessedFlagKey(); // Today's processed folder key
            String processedFolderPrefix = "images/"+ yesterdayProcessedFlagKey;

            try {
                ListObjectsV2Request listRequest = new ListObjectsV2Request()
                        .withBucketName(BUCKET_NAME)
                        .withPrefix(processedFolderPrefix);

                ListObjectsV2Result result;
                List<String> keysToDelete = new ArrayList<>();

                do {
                    result = s3Client.listObjectsV2(listRequest);

                    for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                        String key = objectSummary.getKey();

                        // If the key doesn't match today's processed flag, mark it for deletion
                        if (!key.contains(yesterdayProcessedFlagKey)) {
                            keysToDelete.add(key);
                        }
                    }

                    listRequest.setContinuationToken(result.getNextContinuationToken());
                } while (result.isTruncated());

                // Delete all keys that don't belong to today's processed folder
                if (!keysToDelete.isEmpty()) {
                    DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(BUCKET_NAME)
                            .withKeys(keysToDelete.toArray(new String[0]));
                    s3Client.deleteObjects(deleteRequest);
                    logger.info("Deleted old processed slides: " + keysToDelete.size() + " files");
                } else {
                    logger.info("No old slides to delete");
                }
            } catch (Exception e) {
                logger.error("Error cleaning up old processed slides: " + e.getMessage());
            }
        }

    private String getYesterdayProcessedFlagKey() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("ddMMyy");
        Calendar calendar = Calendar.getInstance();

        // Subtract 1 day from the current date
        calendar.add(Calendar.DATE, -1);

        // Format the date as "ddMMyy"
        String formattedDate = dateFormat.format(calendar.getTime());

        // Return the processed flag key for yesterday
        return PROCESSED_FLAG_KEY_PREFIX + formattedDate;
    }
}



