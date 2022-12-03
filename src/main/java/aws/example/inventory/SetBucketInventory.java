//snippet-sourcedescription:[SetBucketPolicy.java demonstrates how to add a bucket policy to an existing Amazon Simple Storage Service (Amazon S3) bucket.]
//snippet-keyword:[AWS SDK for Java v2]
//snippet-keyword:[Code Sample]
//snippet-service:[Amazon S3]
//snippet-sourcetype:[full-example]
//snippet-sourcedate:[10/28/2020]
//snippet-sourceauthor:[scmacdon-aws]

/*
   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

package aws.example.inventory;

// snippet-start:[s3.java2.set_bucket_policy.import]

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.UUID;
// snippet-end:[s3.java2.set_bucket_policy.import]

public class SetBucketInventory {

    public static void main(String[] args) {

        Region region = Region.US_WEST_2;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        createInventoryConfig(s3, "yoreland-demo", "tpcds", "yorelandivsserver", "inventory3");

        s3.close();
    }

    private static void createDestPrefix(S3Client s3, String destBucketName, String destPrefix){
        s3.putObject(PutObjectRequest.builder()
                .bucket(destBucketName)
                        .key(destBucketName)
                .build(), RequestBody.empty());
    }

    public static void createInventoryConfig(S3Client s3, String bucketName, String prefix, String destBucketName, String destPrefix) {
        createDestPrefix(s3, destBucketName, destPrefix);
        String configID = prefix+UUID.randomUUID().toString().split("-")[0];
        try {
            PutBucketInventoryConfigurationRequest request = PutBucketInventoryConfigurationRequest.builder()
                    .bucket(bucketName)
                    .id(configID)
                    .inventoryConfiguration(InventoryConfiguration.builder()
                            .isEnabled(true)
                            .id(configID)
                            .filter(InventoryFilter.builder()
                                    .prefix(prefix)
                                    .build())
                            .destination(InventoryDestination.builder()
                                    .s3BucketDestination(InventoryS3BucketDestination.builder()
                                            .bucket("arn:aws:s3:::"+destBucketName)
                                            .prefix(destPrefix)
                                            .format(InventoryFormat.CSV)
                                            .build())
                                    .build())
                            .optionalFields(InventoryOptionalField.STORAGE_CLASS,
                                    InventoryOptionalField.LAST_MODIFIED_DATE,
                                    InventoryOptionalField.E_TAG,
                                    InventoryOptionalField.IS_MULTIPART_UPLOADED,
                                    InventoryOptionalField.REPLICATION_STATUS,
                                    InventoryOptionalField.ENCRYPTION_STATUS,
                                    InventoryOptionalField.OBJECT_LOCK_RETAIN_UNTIL_DATE,
                                    InventoryOptionalField.OBJECT_LOCK_MODE,
                                    InventoryOptionalField.OBJECT_LOCK_LEGAL_HOLD_STATUS,
                                    InventoryOptionalField.SIZE)
                            .includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
                            .schedule(InventorySchedule.builder()
                                    .frequency(InventoryFrequency.DAILY)
                                    .build())
                            .build())
                    .build();
            s3.putBucketInventoryConfiguration(request);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        System.out.println("Done!");
    }

}