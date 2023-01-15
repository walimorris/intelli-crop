package com.morris.intellicrop.events;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClient;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.*;
import org.json.JSONObject;

import java.util.List;

public class InvokeIntelliCropWorkFlow {

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

        // List State Machines and collect intelli crop State Machine
        String intelliCropStateMachineArn = null;
        try {

            ListStateMachinesResult listStateMachinesResult = stepFunctionsClient.listStateMachines(new ListStateMachinesRequest());
            List<StateMachineListItem> stateMachines = listStateMachinesResult.getStateMachines();
            if (!stateMachines.isEmpty()) {
                for (StateMachineListItem statemachine : stateMachines) {
                    if (statemachine.getName().equals(System.getenv("STATE_MACHINE"))) {
                        intelliCropStateMachineArn = statemachine.getStateMachineArn();
                        logger.log("StateMachine: " + statemachine.getName());
                        logger.log("StateMachineArn: " + statemachine.getStateMachineArn());
                    }
                }
            }
        } catch (AmazonServiceException e) {
            logger.log("Error processing intelli-crop statemachine step functions request");
        }

        // invoke intelli crop statement step functions workflow and pass bucket and key to first
        // lambda function in the workflow
        if (intelliCropStateMachineArn != null && bucket != null && key != null) {
            JSONObject stepFunctionsInput = new JSONObject();
            stepFunctionsInput.put("bucket", bucket);
            stepFunctionsInput.put("key", key);
            stepFunctionsInput.put("stateMachineArn", intelliCropStateMachineArn);

            StartExecutionRequest startStepFunctionsExecutionRequest = new StartExecutionRequest()
                    .withStateMachineArn(intelliCropStateMachineArn)
                    .withInput(stepFunctionsInput.toString());

            StartExecutionResult startStepFunctionsExecutionResult = stepFunctionsClient.startExecution(startStepFunctionsExecutionRequest);
            logger.log("Function [" + context.getFunctionName() + "], Result: " + startStepFunctionsExecutionResult.toString());
        }

        return "success";
    }
}
