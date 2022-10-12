package com.anthony.dynamodb;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TestQueryItem {

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

        queryTable(enhancedClient);
        queryTableFilter(enhancedClient);
        ddb.close();
    }

    public static void queryTable(DynamoDbEnhancedClient enhancedClient) {

        try{
            DynamoDbTable<Customer> mappedTable = enhancedClient.table("Customer", TableSchema.fromBean(Customer.class));
            QueryConditional queryConditional = QueryConditional.keyEqualTo(Key.builder()
                    .partitionValue("id101")
                    .build());

            // Get items in the table and write out the ID value.
            Iterator<Customer> results = mappedTable.query(queryConditional).items().iterator();
            String result = "";

            while (results.hasNext()) {
                Customer rec = results.next();
                result = rec.getId();
                System.out.println("The record id is " + result);
            }

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public static Integer queryTableFilter(DynamoDbEnhancedClient enhancedClient) {

        Integer countOfCustomers = 0;

        try {
            DynamoDbTable<Customer> mappedTable = enhancedClient.table("Customer", TableSchema.fromBean(Customer.class));

            AttributeValue att = AttributeValue.builder()
                    .s("Anthony")
                    .build();

            Map<String, AttributeValue> expressionValues = new HashMap<>();
            expressionValues.put(":value", att);

            Expression expression = Expression.builder()
                    .expression("custName = :value")
                    .expressionValues(expressionValues)
                    .build();

            // Create a QueryConditional object to query by partitionValue.
            // Since the Customer table has a sort key attribute (email), we can use an expression
            // to filter the query results if multiple items have the same partition key value.
            QueryConditional queryConditional = QueryConditional
                    .keyEqualTo(Key.builder().partitionValue("id122")
                            .build());

            // Perform the query

            for (Customer customer : mappedTable.query(
                    r -> r.queryConditional(queryConditional)
                            .filterExpression(expression)
            ).items()) {
                countOfCustomers++;
                System.out.println(customer);
            }

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        System.out.println("Done");
        return countOfCustomers;
    }
}
