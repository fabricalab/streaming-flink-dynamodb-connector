package it.fabricalab;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class TestUtils {
    public static void createTable(AmazonDynamoDB dynamoDBClient, String tableName, long readCapacityUnits, long writeCapacityUnits,
                                   String partitionKeyName, String partitionKeyType, String sortKeyName, String sortKeyType) {

        try {

            ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement().withAttributeName(partitionKeyName).withKeyType(KeyType.HASH)); // Partition
            // key

            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions
                    .add(new AttributeDefinition().withAttributeName(partitionKeyName).withAttributeType(partitionKeyType));

            if (sortKeyName != null) {
                keySchema.add(new KeySchemaElement().withAttributeName(sortKeyName).withKeyType(KeyType.RANGE)); // Sort
                // key
                attributeDefinitions
                        .add(new AttributeDefinition().withAttributeName(sortKeyName).withAttributeType(sortKeyType));
            }

            CreateTableRequest request = new CreateTableRequest().withTableName(tableName).withKeySchema(keySchema)
                    .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readCapacityUnits)
                            .withWriteCapacityUnits(writeCapacityUnits));


            request.setAttributeDefinitions(attributeDefinitions);

            System.out.println("Issuing CreateTable request for " + tableName);
            CreateTableResult tableResult = dynamoDBClient.createTable(request);


            System.out.println("Waiting for " + tableName + " to be created...this may take a while...");
            TableUtils.waitUntilActive(dynamoDBClient, tableName);
            System.out.println(tableName + " ready.");
        } catch (Exception e) {
            System.err.println("CreateTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
    }

    // Waits for *all* futures to complete and returns a list of results.
    // If *any* future completes exceptionally then the resulting future will also complete exceptionally.

    public static <T> CompletableFuture<List<T>> all(List<CompletableFuture<T>> futures) {
        CompletableFuture[] cfs = futures.toArray(new CompletableFuture[futures.size()]);

        return CompletableFuture.allOf(cfs)
                .thenApply(ignored -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
                );
    }

    public static CompletableFuture<Void> runAsync(Runnable runnable) throws InterruptedException {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        Executors.newCachedThreadPool().submit(() -> {
            runnable.run();
            completableFuture.complete(null);
            return null;
        });

        return completableFuture;
    }

}
