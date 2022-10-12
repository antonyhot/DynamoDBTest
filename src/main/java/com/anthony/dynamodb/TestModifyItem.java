package com.anthony.dynamodb;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class TestModifyItem {

    public static void main(String[] args) {
        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.US_EAST_1;
        DynamoDbClient ddb = DynamoDbClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(region)
                .build();

        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(ddb)
                .build();

        String updatedValue = modifyItem(enhancedClient, "id122", "Anthony@test.com", "Anthony");
        System.out.println("The updated name value is " + updatedValue);
        ddb.close();
    }

    public static String modifyItem(DynamoDbEnhancedClient enhancedClient, String keyVal, String sortKey, String newName) {

        try {

            DynamoDbTable<Customer> mappedTable = enhancedClient.table("Customer", TableSchema.fromBean(Customer.class));
            Key key = Key.builder()
                    .partitionValue(keyVal).sortValue(sortKey)
                    .build();

            // Get the item by using the key and update the email value.
            Customer customerRec = mappedTable.getItem(r -> r.key(key));
            customerRec.setCustName(newName);
            mappedTable.updateItem(customerRec);
            return customerRec.getEmail();

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return "";
    }
}
