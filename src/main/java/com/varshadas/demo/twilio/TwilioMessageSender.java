package com.varshadas.demo.twilio;

import com.twilio.Twilio;
import com.twilio.rest.api.v2010.account.Message;
import com.twilio.type.PhoneNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwilioMessageSender {
    private static final Logger logger = LoggerFactory.getLogger(TwilioMessageSender.class);

    private static final String FROM_WHATSAPP_NUMBER = System.getenv("TWILIO_WHATSAPP_NUMBER") != null
            ? "whatsapp:" + System.getenv("TWILIO_WHATSAPP_NUMBER")
            : null;
    private static final String TO_WHATSAPP_NUMBER = System.getenv("TO_WHATSAPP_NUMBER") != null
            ? "whatsapp:" + System.getenv("TO_WHATSAPP_NUMBER")
            : null;

    static {
        String accountSid = System.getenv("TWILIO_ACCOUNT_SID");
        String authToken = System.getenv("TWILIO_AUTH_TOKEN");

        if (accountSid == null || authToken == null || accountSid.isEmpty() || authToken.isEmpty()) {
            throw new IllegalStateException("Twilio credentials are required.");
        }

        Twilio.init(accountSid, authToken);
        logger.info("Twilio initialized successfully.");
    }

    public void sendWhatsAppMessage(String mediaUrl) {
        if (FROM_WHATSAPP_NUMBER == null || TO_WHATSAPP_NUMBER == null) {
            logger.error("Twilio WhatsApp numbers are not set.");
            throw new IllegalStateException("Both FROM and TO WhatsApp numbers are required.");
        }

        logger.info("Sending WhatsApp message......");
        for (int i = 0; i < 3; i++) {
            try {
                Message.creator(
                                new PhoneNumber(TO_WHATSAPP_NUMBER),
                                new PhoneNumber(FROM_WHATSAPP_NUMBER),
                                "Here is your flashcard for today!")
                        .setMediaUrl(mediaUrl)
                        .create();
                logger.info("WhatsApp message sent successfully with media URL: {}", mediaUrl);
                break; // Break loop if the message is sent successfully
            } catch (Exception e) {
                logger.error("Attempt {}/3 - Error sending WhatsApp message: {}", i + 1, e.getMessage());
                if (i == 2) {
                    // Log failure after 3 attempts
                    logger.error("Failed to send WhatsApp message after 3 attempts.");
                }
                throw new RuntimeException("Failed to send WhatsApp message after 3 attempts.", e);
            }
        }
    }
}
