package com.anthony.dynamodb;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.internal.waiters.ResponseOrException;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

public class TestCreateTable {

    public static void main(String[] args) {
        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.US_EAST_1;
        DynamoDbClient ddb = DynamoDbClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();

        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(ddb)
                .build();

        createTable(enhancedClient, Customer.class);
        createTable(enhancedClient, Music.class);
        ddb.close();
    }

    public static <T> void createTable(DynamoDbEnhancedClient enhancedClient, Class<T> clazz) {
        // Create a DynamoDbTable object
        DynamoDbTable<T> table = enhancedClient.table(clazz.getSimpleName(), TableSchema.fromBean(clazz));
        // Create the table
        table.createTable(builder -> builder
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build())
        );

        System.out.println("Waiting for table creation...");

        try (DynamoDbWaiter waiter = DynamoDbWaiter.create()) { // DynamoDbWaiter is Autocloseable
            ResponseOrException<DescribeTableResponse> response = waiter
                    .waitUntilTableExists(builder -> builder.tableName(clazz.getSimpleName()).build())
                    .matched();
            DescribeTableResponse tableDescription = response.response().orElseThrow(
                    () -> new RuntimeException(clazz.getSimpleName() + " table was not created."));
            // The actual error can be inspected in response.exception()
            System.out.println(tableDescription.table().tableName() + " was created.");
        }
    }
}
