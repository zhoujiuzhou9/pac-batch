package com.cainz.batch;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.cainz.dto.Promotion;
import com.cainz.service.InsertToDB;
import com.cainz.service.InsertToDBImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class S3EventHandler implements RequestHandler<S3Event, Boolean> {
    private static final AmazonS3 s3Client = AmazonS3Client.builder()
            .withCredentials(new DefaultAWSCredentialsProviderChain())
            .build();

    @Override
    public Boolean handleRequest(S3Event input, Context context) {
        final LambdaLogger logger = context.getLogger();

        //check if are getting any record
        if (input.getRecords().isEmpty()) {
            logger.log("No records found");
            return false;
        }

        //process the records
//        for (S3EventNotification.S3EventNotificationRecord record : input.getRecords()) {
//            String bucketName = record.getS3().getBucket().getName();
//            String objectKey = record.getS3().getObject().getKey();
//            logger.log("bucketName: " + bucketName);
//            logger.log("objectKey: " + objectKey);
//            S3Object s3Object = s3Client.getObject(bucketName, objectKey);
//            S3ObjectInputStream inputStream = s3Object.getObjectContent();
//            try (final BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
//                br.lines().skip(1)
//                        .forEach(line -> logger.log(line + "\n"));
//            } catch (IOException e) {
//                logger.log("Error occurred in Lambda:" + e.getMessage());
//                return false;
//            }
//        }


        S3Object s3Object = s3Client.getObject("common-use-for-andre-and-all", "cainz.csv");
        S3ObjectInputStream inputStream = s3Object.getObjectContent();
        //processing CSV - open CSV, apache CSV
        InsertToDB insertToDB=new InsertToDBImpl();
        List<Promotion> proList=null;
        try (final BufferedReader br =
                     new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String data;
            Promotion Promotion=new Promotion();

            while ((data = br.readLine()) != null) {
                logger.log(data + "\n");
                Promotion.setStoreCode("Data.xxx");
                proList.add(Promotion);
                //data-->Promotion
                //Promotion-->List(Promotion)
                //List<Promotion> proList=null;
            }

        } catch (IOException e) {
            logger.log("Error occurred in Lambda:" + e.getMessage());
            return false;
        }
        insertToDB.insertToDB(proList);



        S3Object s3Object2 = s3Client.getObject("common-use-for-andre-and-all", "cainz2.csv");
        S3ObjectInputStream inputStream2 = s3Object2.getObjectContent();
        //processing CSV - open CSV, apache CSV

        try (final BufferedReader br2 = new BufferedReader(new InputStreamReader(inputStream2, StandardCharsets.UTF_8))) {
            br2.lines().skip(1)
                    .forEach(line -> logger.log(line + "\n"));
        } catch (IOException e) {
            logger.log("Error occurred in Lambda:" + e.getMessage());
            return false;
        }

        return true;
    }
}
