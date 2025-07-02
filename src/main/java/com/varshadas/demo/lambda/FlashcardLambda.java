package com.varshadas.demo.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.varshadas.demo.s3.S3CleanupService;
import com.varshadas.demo.s3.S3FileHandler;
import com.varshadas.demo.utils.SlidePreprocessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class FlashcardLambda implements RequestHandler<ScheduledEvent, String> {

    private static final Logger logger = LoggerFactory.getLogger(FlashcardLambda.class);
    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    private final S3FileHandler s3FileHandler = new S3FileHandler();
    private final SlidePreprocessor slidePreprocessor = new SlidePreprocessor();
    private final S3CleanupService cleanupService = new S3CleanupService();

    private static final String PPT_FILENAME = System.getenv("PPT_FILENAME");

    @Override
    public String handleRequest(ScheduledEvent event, Context context) {
        logger.info("event = {}", event);

        try {
            if (PPT_FILENAME == null || PPT_FILENAME.isEmpty()) {
                logger.error("PPT_FILENAME environment variable is not set.");
                throw new IllegalStateException("PPT_FILENAME is required.");
            }

            String todayFlagKey = s3FileHandler.getTodayProcessedFlagKey();

            if (!s3FileHandler.isSlidesPreprocessed(s3Client, todayFlagKey)) {
                logger.info("Downloading file {} from bucket", PPT_FILENAME);
                File pptFile = null;

                for (int i = 0; i < 3; i++) {
                    try {
                        pptFile = s3FileHandler.downloadFileFromS3(s3Client, PPT_FILENAME);
                        if (pptFile != null)
                            break;
                    } catch (Exception e) {
                        logger.warn("Retry {}/3 failed to download PPT file.", i + 1);
                    }
                    Thread.sleep(2000); // Add delay between retries
                }

                if (pptFile == null) {
                    logger.error("Failed to download PowerPoint file after 3 attempts");
                    return "Error: Failed to download PowerPoint file";
                }

                slidePreprocessor.preprocessSlides(s3Client, pptFile, todayFlagKey);
                s3FileHandler.markSlidesAsProcessed(s3Client, todayFlagKey);
                cleanupService.cleanupPreviousDayProcessedSlides(s3Client);
            }

            slidePreprocessor.processSlidesForTheDay(s3Client, todayFlagKey);

        } catch (Exception e) {
            logger.error("Error processing PowerPoint file: " + e.getMessage(), e);
            return "Error: " + e.getMessage();
        }

        return "Messages sent successfully!";
    }
}
