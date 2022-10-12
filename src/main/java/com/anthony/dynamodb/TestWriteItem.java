package com.anthony.dynamodb;

import org.apache.commons.lang3.RandomStringUtils;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TestWriteItem {

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

        putBatchRecords(enhancedClient);
        ddb.close();
    }

    public static void putBatchRecords(DynamoDbEnhancedClient enhancedClient) {

        try {
            DynamoDbTable<Customer> customerMappedTable = enhancedClient.table("Customer", TableSchema.fromBean(Customer.class));
            DynamoDbTable<Music> musicMappedTable = enhancedClient.table("Music", TableSchema.fromBean(Music.class));

            LocalDate localDate = LocalDate.parse("2020-04-07");
            LocalDateTime localDateTime = localDate.atStartOfDay();
            Instant instant = localDateTime.toInstant(ZoneOffset.UTC);

            // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
            List<WriteBatch> writeBatches = new ArrayList<>();
            for (int i = 100; i < 1000; i++) {
                Customer record = new Customer();
                String name = RandomStringUtils.randomAlphanumeric(5).toUpperCase();
                record.setCustName(name);
                record.setId("id" + i);
                if (Objects.equals(i, 122)) {
                    record.setEmail("Anthony@test.com");
                } else {
                    record.setEmail(name + "@test.com");
                }
                record.setRegDate(instant);

                // add items to the Customer table
                WriteBatch writeBatch = WriteBatch.builder(Customer.class)
                        .mappedTableResource(customerMappedTable)
                        .addPutItem(builder -> builder.item(record)).build();
                writeBatches.add(writeBatch);

                if (writeBatches.size() == 24) {
                    System.out.println("Start to insert item from " + (i - 23) + " to " + i);
                    insert(enhancedClient, writeBatches);
                    writeBatches.clear();
                }
            }

            // delete an item from the Music table
            WriteBatch writeBatch = WriteBatch.builder(Music.class)
                    .mappedTableResource(musicMappedTable)
                            .addDeleteItem(builder -> builder.key(Key.builder().partitionValue("Famous Bond").build())).build();
            writeBatches.add(writeBatch);

            System.out.println("Start to insert the last " + writeBatches.size() + " item");
            insert(enhancedClient, writeBatches);

            System.out.println("done");

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private static void insert(DynamoDbEnhancedClient enhancedClient, List<WriteBatch> writeBatches) {
        BatchWriteItemEnhancedRequest batchWriteItemEnhancedRequest = BatchWriteItemEnhancedRequest.builder()
                .writeBatches(writeBatches)
                .build();

        // Add two items to the Customer table and delete one item from the Music table
        enhancedClient.batchWriteItem(batchWriteItemEnhancedRequest);
    }
}
