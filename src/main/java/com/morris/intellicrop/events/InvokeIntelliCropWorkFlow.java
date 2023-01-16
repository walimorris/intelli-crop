package com.morris.intellicrop.events;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClient;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.ListStateMachinesRequest;
import com.amazonaws.services.stepfunctions.model.ListStateMachinesResult;
import com.amazonaws.services.stepfunctions.model.StateMachineListItem;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;

import org.json.JSONObject;
import java.util.List;

public class InvokeIntelliCropWorkFlow {
    public static final String BUCKET = "bucket";
    public static final String KEY = "key";
    public static final String STATE_MACHINE_ARN = "stateMachineArn";

    public String handleRequest(S3Event event, Context context) {
        LambdaLogger logger = context.getLogger();
        S3EventNotification.S3EventNotificationRecord record = event.getRecords().get(0);
        String bucket = record.getS3().getBucket().getName();
        String key = record.getS3().getObject().getKey();

        Regions region = Regions.fromName(System.getenv("REGION"));
        AWSStepFunctionsClientBuilder stepFunctionsClientBuilder = AWSStepFunctionsClientBuilder
                .standard()
                .withRegion(region);

        AWSStepFunctionsClient stepFunctionsClient = (AWSStepFunctionsClient) stepFunctionsClientBuilder.build();
        String intelliCropStateMachineArn = getIntelliCropStateMachineArn(stepFunctionsClient, logger);

        if (intelliCropStateMachineArn != null && bucket != null && key != null) {
            startIntelliCropWorkflow(stepFunctionsClient, intelliCropStateMachineArn, bucket, key, logger, context);
        } else {
            logger.log("Check that this function consumes the correct workflow input for a successful process.");
        }

        stepFunctionsClient.shutdown();
        return "success";
    }

    /**
     * Uses the {@link AWSStepFunctionsClient} to query AWS Step Functions Service for the intelli-crop
     * statemachine ARN, given the statemachine name.
     *
     * @param stepFunctionsClient {@link AWSStepFunctionsClient}
     * @param logger {@link LambdaLogger}
     *
     * @return {@link String} intelli-crop statemachine ARN
     */
    private String getIntelliCropStateMachineArn(AWSStepFunctionsClient stepFunctionsClient, LambdaLogger logger) {
        try {
            ListStateMachinesResult listStateMachinesResult = stepFunctionsClient.listStateMachines(new ListStateMachinesRequest());
            List<StateMachineListItem> stateMachines = listStateMachinesResult.getStateMachines();
            if (!stateMachines.isEmpty()) {
                for (StateMachineListItem statemachine : stateMachines) {
                    if (statemachine.getName().equals(System.getenv("STATE_MACHINE"))) {
                        return statemachine.getStateMachineArn();
                    }
                }
            }
        } catch (AmazonServiceException e) {
            logger.log("Error processing intelli-crop statemachine step functions request: " + e.getErrorMessage());
        }
        return null;
    }

    /**
     * Initiates the intelli-crop Step Function Workflow given the input from a user uploading an image
     * to the intelli-crop image upload bucket.
     *
     * @param stepFunctionsClient {@link AWSStepFunctionsClient}
     * @param intelliCropStateMachineArn intelli-crop statemachine arn
     * @param bucket intelli-crop input bucket
     * @param key uploaded S3 image object key
     * @param logger {@link LambdaLogger}
     * @param context {@link Context}
     */
    private void startIntelliCropWorkflow(AWSStepFunctionsClient stepFunctionsClient, String intelliCropStateMachineArn, String bucket, String key,
                                          LambdaLogger logger, Context context) {

        JSONObject stepFunctionsInput = new JSONObject();
        stepFunctionsInput.put(BUCKET, bucket);
        stepFunctionsInput.put(KEY, key);
        stepFunctionsInput.put(STATE_MACHINE_ARN, intelliCropStateMachineArn);

        StartExecutionRequest startStepFunctionsExecutionRequest = new StartExecutionRequest()
                .withStateMachineArn(intelliCropStateMachineArn)
                .withInput(stepFunctionsInput.toString());

        StartExecutionResult startStepFunctionsExecutionResult = stepFunctionsClient.startExecution(startStepFunctionsExecutionRequest);
        logger.log("Function [" + context.getFunctionName() + "], Result: " + startStepFunctionsExecutionResult.toString());
    }
}
