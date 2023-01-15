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
import java.util.Map;

public class CropImageToBoundingBox {

    public String handleRequest(Map<String, Object> input, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Inside Cropping image function");
        Regions region = Regions.fromName(System.getenv("REGION"));
        String outputBucket = System.getenv("OUTPUT_BUCKET");

        AmazonS3Client s3Client = (AmazonS3Client) AmazonS3ClientBuilder.standard().withRegion(region).build();
        Map<String, String> taskResult = (Map<String, String>) input.get("taskresult1");

        String bucket = input.get("bucket").toString();
        String key = input.get("key").toString();

        String[] fileTypeSplit = key.split("\\.");
        String fileType = fileTypeSplit[fileTypeSplit.length - 1];

        double top = Double.parseDouble(taskResult.get("top"));
        double left = Double.parseDouble(taskResult.get("left"));
        double width = Double.parseDouble(taskResult.get("width"));
        double height = Double.parseDouble(taskResult.get("height"));

        S3Object object = s3Client.getObject(new GetObjectRequest(bucket, key));

        try {
            BufferedImage originalImg = ImageIO.read(object.getObjectContent());

            int x = (int) Math.abs((originalImg.getWidth() * left));
            int y = (int) Math.abs((originalImg.getHeight() * top));;
            int w = (int) Math.abs((originalImg.getWidth() * width));
            int h = (int) Math.abs((originalImg.getHeight() * height));

            int finalX = x + w;
            int finalH = y + h;

            if (finalX > originalImg.getWidth()) {
                w = originalImg.getWidth() - x;
            }

            if (finalH > originalImg.getHeight()) {
                h = originalImg.getHeight() - y;
            }

            BufferedImage subImg = originalImg.getSubimage(x, y, w, h);

            ByteArrayOutputStream os = new ByteArrayOutputStream();
            ImageIO.write(subImg, fileType, os);
            byte[] buffer = os.toByteArray();
            InputStream is = new ByteArrayInputStream(buffer);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(buffer.length);
            meta.setContentType("image/" + fileType);
            s3Client.putObject(new PutObjectRequest(outputBucket, key, is, meta));
        } catch (IOException e) {
            logger.log("Error cropping uploaded image");
        }

        return "success";
    }
}
