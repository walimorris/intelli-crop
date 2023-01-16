package com.morris.intellicrop.events;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class CropImageToBoundingBox {
    public static final String TASK_RESULT = "taskresult";
    public static final String BUCKET = "bucket";
    public static final String KEY = "key";

    public static final String TOP = "top";
    public static final String LEFT = "left";
    public static final String HEIGHT = "height";
    public static final String WIDTH = "width";

    public String handleRequest(Map<String, Object> input, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Inside Cropping image function");
        Regions region = Regions.fromName(System.getenv("REGION"));
        String outputBucket = System.getenv("OUTPUT_BUCKET");

        AmazonS3Client s3Client = (AmazonS3Client) AmazonS3ClientBuilder
                .standard()
                .withRegion(region)
                .build();

        Map<String, String> taskResult = (Map<String, String>) input.get(TASK_RESULT);

        String bucket = input.get(BUCKET).toString();
        String key = input.get(KEY).toString();

        String fileType = getImageFileType(key);
        Map<String, Double> taskResultMap = storeBoundingBoxProperties(taskResult);

        S3Object object = s3Client.getObject(new GetObjectRequest(bucket, key));
        BufferedImage subImage = getSubImageFromOriginalImage(object, taskResultMap, logger);
        putCroppedImageToOutputBucket(s3Client, subImage, fileType, outputBucket, key, logger);

        return "success";
    }

    /**
     * Get image file type from given {@link String} key.
     *
     * @param imageKey {@link String} S3Object key
     * @return {@link String} file type
     */
    private String getImageFileType(String imageKey) {
        String[] fileTypeSplit = imageKey.split("\\.");
        return fileTypeSplit[fileTypeSplit.length - 1];
    }

    /**
     * Store bounding-box properties from the given task result input in step functions workflow.
     *
     * @param taskResult {@link Map} containing step functions input task result
     * @return {@link Map} containing bounding-box properties
     */
    private Map<String, Double> storeBoundingBoxProperties(Map<String, String> taskResult) {
       Map<String, Double> taskResultMap = new HashMap<>();

       taskResultMap.put(TOP, Double.parseDouble(taskResult.get(TOP)));
       taskResultMap.put(LEFT, Double.parseDouble(taskResult.get(LEFT)));
       taskResultMap.put(WIDTH, Double.parseDouble(taskResult.get(WIDTH)));
       taskResultMap.put(HEIGHT, Double.parseDouble(taskResult.get(HEIGHT)));

       return taskResultMap;
    }

    /**
     * Get cropped sub image from given origin image from {@link S3Object}.
     *
     * @param originalImageObject {@link S3Object} original image from S3
     * @param taskResultsMap {@link Map} containing bounding-box properties
     * @param logger {@link LambdaLogger}
     *
     * @return {@link BufferedImage} cropped image
     */
    private BufferedImage getSubImageFromOriginalImage(S3Object originalImageObject, Map<String, Double> taskResultsMap,
                                                       LambdaLogger logger) {

        BufferedImage subImage = null;
        try {
            BufferedImage originalImg = ImageIO.read(originalImageObject.getObjectContent());

            int x = (int) Math.abs((originalImg.getWidth() * taskResultsMap.get(LEFT)));
            int y = (int) Math.abs((originalImg.getHeight() * taskResultsMap.get(TOP)));
            int w = (int) Math.abs((originalImg.getWidth() * taskResultsMap.get(WIDTH)));
            int h = (int) Math.abs((originalImg.getHeight() * taskResultsMap.get(HEIGHT)));

            int finalX = x + w;
            int finalH = y + h;

            if (finalX > originalImg.getWidth()) {
                w = originalImg.getWidth() - x;
            }

            if (finalH > originalImg.getHeight()) {
                h = originalImg.getHeight() - y;
            }
            subImage = originalImg.getSubimage(x, y, w, h);

        } catch (IOException e) {
            logger.log("Error cropping original image: " + e.getMessage());
        }
        return subImage;
    }

    /**
     * Upload cropped image to intelli-crop output S3 Bucket.
     *
     * @param s3Client {@link AmazonS3Client}
     * @param subImage {@link BufferedImage} sub image
     * @param fileType image file type
     * @param outputBucket output bucket where cropped image will be uploaded
     * @param key image key
     * @param logger {@link LambdaLogger}
     */
    private void putCroppedImageToOutputBucket(AmazonS3Client s3Client, BufferedImage subImage, String fileType,
                                               String outputBucket, String key, LambdaLogger logger) {

        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            ImageIO.write(subImage, fileType, os);
            byte[] buffer = os.toByteArray();
            InputStream is = new ByteArrayInputStream(buffer);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(buffer.length);
            meta.setContentType("image/" + fileType);
            s3Client.putObject(new PutObjectRequest(outputBucket, key, is, meta));
        } catch (IOException e) {
            logger.log("Error uploading cropped image " + key + " to bucket " + outputBucket + ": " + e.getMessage());
        }
    }
}
