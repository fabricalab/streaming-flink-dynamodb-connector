package it.fabricalab;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import it.fabricalab.flink.dynamodb.sink.AugmentedWriteRequest;
import it.fabricalab.flink.dynamodb.sink.DynamoDBProducer;
import it.fabricalab.flink.dynamodb.sink.DynamoDBProducerConfiguration;
import it.fabricalab.flink.dynamodb.sink.FlinkDynamoDBProducer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.dynamodb.DynaliteContainer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

        //count inserted items
        //adesso guardiamo cosa c'e' nella tabella
        List<Map<String, AttributeValue>> items = new ArrayList<>();

        ScanRequest sr = new ScanRequest()
                .withTableName("TempTableName")
                .withAttributesToGet(Arrays.asList("KeyName", "anotherAttribute"));

        while (true) {
            ScanResult scanResult = dynamoDBClient.scan( sr );

            items.addAll(scanResult.getItems());
            if(scanResult.getLastEvaluatedKey()  == null || scanResult.getLastEvaluatedKey().isEmpty() )
                break;
            else
                sr.setExclusiveStartKey( scanResult.getLastEvaluatedKey() );
        }
        assertEquals(30, items.size());
        System.err.println(items.size());
        System.err.println(items);
    }

    @Test
    public void testFail() throws Exception {

        String EXC_MESSAGE ="Intended Exception for testing";

        try {

        //creiamo una rete con solo il nostor sink
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        List<AugmentedWriteRequest> list = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            list.add(createAugmentedWriteRequest("TempTableName", String.format("key-%04d", i), "XYZ"));
        }

        DataStreamSource<AugmentedWriteRequest> streamIn = env.fromCollection(list);

        FlinkDynamoDBProducer mySink = new FlinkDynamoDBProducer(new Properties()) {
            @Override
            protected Client getClient(DynamoDBProducerConfiguration producerConfig) {
                /***
                 * Create client Throwing exception
                 */
                return new Client() {
                    @Override
                    public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequestx) {
                        throw new RuntimeException(EXC_MESSAGE);
                    }
                };
            }
        };

        streamIn.addSink(mySink);

        env.execute("E2E Failing Test for FlinkDynamoDBProducer");


        } catch (Throwable  e) {

            Throwable root = e;
            while(root.getCause() != null) {
                System.err.println("recurring from: " + root.getCause());
                root = root.getCause();
            }
            assertEquals(root.getMessage(), EXC_MESSAGE);

        }


    }




}
