package com.morris.intellicrop.events;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;

import java.util.HashMap;
import java.util.Map;

public class DetectBoundingBoxImageMetaData {

    public Map<String, String> handleRequest(Map<String, Object> input, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Detect Image Meta Data Function in StateMachine has been triggered");
        Regions region = Regions.fromName(System.getenv("REGION"));

        if (input != null) {
            String bucket = (String) input.get("bucket");
            String key = (String) input.get("key");

            AmazonRekognition amazonRekognitionClient = AmazonRekognitionClientBuilder
                    .standard()
                    .withRegion(region)
                    .build();

            RecognizeCelebritiesRequest request = new RecognizeCelebritiesRequest()
                    .withImage(new Image().withS3Object(new S3Object().withName(key).withBucket(bucket)));

            RecognizeCelebritiesResult result = amazonRekognitionClient.recognizeCelebrities(request) ;
            ComparedFace face = result.getUnrecognizedFaces().get(0);
            BoundingBox boundingBox = face.getBoundingBox();

            Map<String, String> output = new HashMap<>();
            if (boundingBox != null) {
                output.put("top", String.valueOf(boundingBox.getTop()));
                output.put("height", String.valueOf(boundingBox.getHeight()));
                output.put("left", String.valueOf(boundingBox.getLeft()));
                output.put("width", String.valueOf(boundingBox.getWidth()));
            }
            return output;

        }
        return null;
    }
}
