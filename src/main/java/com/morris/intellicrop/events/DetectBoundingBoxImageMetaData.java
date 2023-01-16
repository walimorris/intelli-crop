package com.morris.intellicrop.events;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.BoundingBox;
import com.amazonaws.services.rekognition.model.ComparedFace;
import com.amazonaws.services.rekognition.model.RecognizeCelebritiesResult;
import com.amazonaws.services.rekognition.model.RecognizeCelebritiesRequest;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.rekognition.model.Image;

import java.util.HashMap;
import java.util.Map;

public class DetectBoundingBoxImageMetaData {
    public static final String BUCKET = "bucket";
    public static final String KEY = "key";
    public static final String TOP = "top";
    public static final String LEFT = "left";
    public static final String HEIGHT = "height";
    public static final String WIDTH = "width";

    public Map<String, String> handleRequest(Map<String, Object> input, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Detect Image Meta Data Function in Intelli-Crop StateMachine has been triggered");
        Regions region = Regions.fromName(System.getenv("REGION"));

        BoundingBox boundingBox = null;
        if (input != null) {
            String bucket = (String) input.get(BUCKET);
            String key = (String) input.get(KEY);

            AmazonRekognition amazonRekognitionClient = AmazonRekognitionClientBuilder
                    .standard()
                    .withRegion(region)
                    .build();

            boundingBox = analyzeImageForBoundingBox(amazonRekognitionClient, bucket, key, logger);
        }
        return buildStepFunctionOutput(boundingBox, logger);
    }

    /**
     * Utilizes the Amazon Rekognition {@link AmazonRekognition} machine learning algorithm to analyze
     * the given image and produces a bounding box of the face in the image.
     *
     * @param rekognitionClient {@link AmazonRekognition}
     * @param bucket S3 input bucket
     * @param key S3 image key
     * @param logger {@link LambdaLogger}
     *
     * @return {@link BoundingBox}
     */
    private BoundingBox analyzeImageForBoundingBox(AmazonRekognition rekognitionClient, String bucket, String key,
                                                   LambdaLogger logger) {

        RecognizeCelebritiesRequest request = new RecognizeCelebritiesRequest()
                .withImage(new Image().withS3Object(new S3Object()
                        .withName(key)
                        .withBucket(bucket)));

        RecognizeCelebritiesResult result = rekognitionClient.recognizeCelebrities(request);

        ComparedFace face = null;

        // check to ensure uploaded image is not a celebrity
        if (!result.getUnrecognizedFaces().isEmpty()) {
            face = result.getUnrecognizedFaces().get(0);

        // check if given image is a celebrity
        } else if (!result.getCelebrityFaces().isEmpty()) {
            face = result.getCelebrityFaces().get(0).getFace();
        } else {
            logger.log("Rekognition can not provide a bounding box attributes from uploaded image");
            return null;
        }
        return face.getBoundingBox();
    }

    /**
     * Builds Intelli-Crop Step Function output for the DetectBoundingBox portion of the intelli-crop
     * statemachine workflow or gives a clear message if bounding-box does not exist.
     *
     * @param boundingBox {@link BoundingBox}
     * @param logger {@link LambdaLogger}
     *
     * @return {@link Map} of bounding-box properties
     */
    private Map<String, String> buildStepFunctionOutput(BoundingBox boundingBox, LambdaLogger logger) {
        Map<String, String> output = new HashMap<>();
        if (boundingBox != null) {
            output.put(TOP, String.valueOf(boundingBox.getTop()));
            output.put(HEIGHT, String.valueOf(boundingBox.getHeight()));
            output.put(LEFT, String.valueOf(boundingBox.getLeft()));
            output.put(WIDTH, String.valueOf(boundingBox.getWidth()));

            return output;
        }
        logger.log("Empty bounding-box results for uploaded image in intelli-crop workflow");
        return null;
    }
}
