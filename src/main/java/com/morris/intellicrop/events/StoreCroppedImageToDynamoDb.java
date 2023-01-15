package com.morris.intellicrop.events;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class StoreCroppedImageToDynamoDb {

    public String handleRequest(Map<String, Object> input, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("inside StoreCroppedImageToDynamoDb");

        Regions region = Regions.fromName(System.getenv("REGION"));
        String outputBucket = System.getenv("OUTPUT_BUCKET");
        String dynamoTable = System.getenv("DYNAMO_TABLE");
        String key = input.get("key").toString();

        AmazonS3Client s3Client = (AmazonS3Client) AmazonS3ClientBuilder
                .standard()
                .withRegion(region)
                .build();

        AmazonDynamoDBClient dynamoDBClient = (AmazonDynamoDBClient) AmazonDynamoDBClient
                .builder()
                .withRegion(region)
                .build();

        // get object s3 url in output bucket
        String imageUrl = s3Client.getResourceUrl(outputBucket, key);

        // create unique id for image item
        String uniqueId = UUID.randomUUID().toString();

        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put("Id", new AttributeValue(uniqueId));
        itemValues.put("S3Url", new AttributeValue(imageUrl));

        try {
            dynamoDBClient.putItem(dynamoTable, itemValues);
        } catch (ResourceNotFoundException e) {
            logger.log("Error uploading given items to " + dynamoTable + " table");
        }

        return "success";
    }
}
