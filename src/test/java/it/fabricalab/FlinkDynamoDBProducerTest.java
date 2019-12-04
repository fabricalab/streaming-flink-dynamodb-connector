package it.fabricalab;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import it.fabricalab.flink.dynamodb.sink.AugmentedWriteRequest;
import it.fabricalab.flink.dynamodb.sink.DynamoDBProducer;
import it.fabricalab.flink.dynamodb.sink.DynamoDBProducerConfiguration;
import it.fabricalab.flink.dynamodb.sink.FlinkDynamoDBProducer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.dynamodb.DynaliteContainer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class FlinkDynamoDBProducerTest {

    @ClassRule
    public static DynaliteContainer dynamoDB = new DynaliteContainer();


    public static AugmentedWriteRequest createAugmentedWriteRequest(String table, String key, String another) {
        Map<String, AttributeValue> map = new HashMap<>();

        map.put("KeyName", new AttributeValue(key));
        map.put("anotherKey", new AttributeValue(another));


        return new AugmentedWriteRequest(table,
                new WriteRequest().withPutRequest(new PutRequest().withItem(map)));

    }

    @Test
    public void testProducer() throws Exception {


        //creiamo la tabella
        dynamoDB.start();
        AmazonDynamoDB dynamoDBClient = dynamoDB.getClient();

        //to stay serializable we must create the client inside the
        //operator

        String containerIpAddress = dynamoDB.getContainerIpAddress();
        int mappedPort = dynamoDB.getMappedPort(4567);


        TestUtils.createTable(dynamoDBClient, "TempTableName", 5, 5,
                "KeyName", "S", null, null);


        //creiamo una rete con solo il nostor sink
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<AugmentedWriteRequest> list = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            list.add(
                    createAugmentedWriteRequest("TempTableName", String.format("key-%04d", i), "XYZ"));
        }

        DataStreamSource<AugmentedWriteRequest> streamIn = env.fromCollection(list);

        streamIn.addSink(
                new FlinkDynamoDBProducer(new Properties()) {
                    @Override
                    protected Client getClient(DynamoDBProducerConfiguration producerConfig) {
                        /***
                         * Create client
                         */
                        AmazonDynamoDB innerDynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                                .withEndpointConfiguration(
                                        new AwsClientBuilder.EndpointConfiguration(
                                                "http://" + containerIpAddress + ":" + mappedPort, null))
                                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummy", "dummy")))
                                .build();
                        return innerDynamoDBClient::batchWriteItem;
                    }
                }
        );

        env.execute("E2E Test for FlinkDynamoDBProducer");




//
//        BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest();
//
//        Map<String, List<WriteRequest>> mapOfTablesAndRequests = new HashMap<>();
//
//        List<WriteRequest> listOfRequests = new ArrayList<>();
//
//        Map<String, AttributeValue> item = new HashMap<>();
//        item.put("key1", new AttributeValue("value1"));
//        item.put("key2", new AttributeValue("value2"));
//
//        WriteRequest writeRequest = new WriteRequest( new PutRequest(item));
//        listOfRequests.add(writeRequest);
//
//        mapOfTablesAndRequests.put("tableName", listOfRequests);
//
//
//        batchWriteItemRequest.setRequestItems( mapOfTablesAndRequests );
//
//        //BatchWriteItemResult batchWriteItemResult = testClient.batchWriteItem(batchWriteItemRequest);
//
//
//        //Map<String, List<WriteRequest>> unprocessedItems = batchWriteItemResult.getUnprocessedItems();
////
////        CompletableFuture<BatchWriteItemResult> completableFuture =
////                CompletableFuture.supplyAsync(
////                        () -> testClient.batchWriteItem(batchWriteItemRequest)
////                ).handleAsync(
////                        (batchWriteItemResult, error) ->  {
////                            if(error != null) {
////
////                            } else {
////                                Map<String, List<WriteRequest>> unprocessedItems =
////                                                          batchWriteItemResult.getUnprocessedItems();
////
////                            }
////                        });
////                )
////
////
////
////        if(unprocessedItems != null && unprocessedItems.isEmpty() ) {
////            enqueue;
////        }
//
//        //arrivano coppue di <tablename,writerequest>
//        //noi manteniamo una writerequest e di aggiungiamo
//        //le wr che arrivano
//        //quando arriviamo ad numero prefissato (<25 per spec)
//        //le mettiamo nella lista di quelle da scrivere
//        //ed un thread le prende da quella lista e le scrive
//        //se ritorna che le ha processate tutte siamo tranquilli
//        //altrimenti bisogna che quelli 'rimanenti' finiscano di nuovo nella lista
//        //se e' throttling deve anche chiedere al throttling service di farlo rallentare !


    }
}
