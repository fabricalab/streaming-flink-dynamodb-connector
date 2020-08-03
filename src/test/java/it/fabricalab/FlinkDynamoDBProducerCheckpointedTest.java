package it.fabricalab;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTyped;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.util.StringUtils;
import it.fabricalab.flink.dynamodb.sink.AugmentedWriteRequest;
import it.fabricalab.flink.dynamodb.sink.FlinkDynamoDBProducer;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.dynamodb.DynaliteContainer;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlinkDynamoDBProducerCheckpointedTest {

    @ClassRule
    public static DynaliteContainer dynamoDB = new DynaliteContainer();


    public static AugmentedWriteRequest createAugmentedWriteRequest(String table, String key, String another, String keyName) {
        Map<String, AttributeValue> map = new HashMap<>();

        map.put(keyName, new AttributeValue(key));
        map.put("anotherKey", new AttributeValue(another));


        return new AugmentedWriteRequest(table,
                new WriteRequest().withPutRequest(new PutRequest().withItem(map)));

    }

    public static AugmentedWriteRequest createAugmentedWriteRequest(String table, String key, String another) {
        return createAugmentedWriteRequest(table, key, another, "KeyName");
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

        env.setStateBackend((StateBackend) new RocksDBStateBackend("/tmp/flinkTestStateRocksdb"));

        List<AugmentedWriteRequest> list = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            list.add(
                    createAugmentedWriteRequest("TempTableName", String.format("key-%04d", i), "XYZ"));
        }

        DataStreamSource<AugmentedWriteRequest> streamIn = env.fromCollection(list);

        Properties properties = new Properties();
        properties.setProperty(FlinkDynamoDBProducer.DYNAMODB_PRODUCER_CHECKPOINTINGMODE_PROPERTY,
                FlinkDynamoDBProducer.CheckpointingMode.ListState.name());

        streamIn.addSink(
                new FlinkDynamoDBProducer(properties) {
                    @Override
                    protected Client getClient(Properties producerProps) {
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
            ScanResult scanResult = dynamoDBClient.scan(sr);

            items.addAll(scanResult.getItems());
            if (scanResult.getLastEvaluatedKey() == null || scanResult.getLastEvaluatedKey().isEmpty())
                break;
            else
                sr.setExclusiveStartKey(scanResult.getLastEvaluatedKey());
        }
        assertEquals(30, items.size());
        System.err.println(items.size());
        System.err.println(items);
    }


    @Test
    public void testProducerTimelyFlush() throws Exception {


        //creiamo la tabella
        dynamoDB.start();
        AmazonDynamoDB dynamoDBClient = dynamoDB.getClient();

        //to stay serializable we must create the client inside the
        //operator

        String containerIpAddress = dynamoDB.getContainerIpAddress();
        int mappedPort = dynamoDB.getMappedPort(4567);


        TestUtils.createTable(dynamoDBClient, "TempTableName", 5, 5,
                "KeyName", "S", null, null);


        //creiamo una rete con solo il nostro sink
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend((StateBackend) new RocksDBStateBackend("/tmp/flinkTestStateRocksdb"));


        List<AugmentedWriteRequest> list = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            list.add(
                    createAugmentedWriteRequest("TempTableName", String.format("key-%04d", i), "XYZ"));
        }

        DataStreamSource<AugmentedWriteRequest> streamIn = env.fromCollection(list);

        //never use 10 milliseconds in a real job, just to stress the timer machinery.
        Properties properties = new Properties();
        properties.setProperty(FlinkDynamoDBProducer.DYNAMODB_PRODUCER_CHECKPOINTINGMODE_PROPERTY,
                FlinkDynamoDBProducer.CheckpointingMode.ListState.name());

        properties.setProperty(FlinkDynamoDBProducer.DYNAMODB_PRODUCER_TIMEOUT_PROPERTY, "10");

        streamIn.addSink(
                new FlinkDynamoDBProducer(properties) {
                    @Override
                    protected Client getClient(Properties producerProps) {
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
            ScanResult scanResult = dynamoDBClient.scan(sr);

            items.addAll(scanResult.getItems());
            if (scanResult.getLastEvaluatedKey() == null || scanResult.getLastEvaluatedKey().isEmpty())
                break;
            else
                sr.setExclusiveStartKey(scanResult.getLastEvaluatedKey());
        }
        assertEquals(3000, items.size());
        System.err.println(items.size());
        System.err.println(items);
    }


    @Test
    public void testProducerAndConflict() throws Exception {


        //creiamo la tabella
        dynamoDB.start();
        AmazonDynamoDB dynamoDBClient = dynamoDB.getClient();

        //to stay serializable we must create the client inside the
        //operator

        String containerIpAddress = dynamoDB.getContainerIpAddress();
        int mappedPort = dynamoDB.getMappedPort(4567);


        TestUtils.createTable(dynamoDBClient, "TempTableNameConflict", 5, 5,
                "KeyName", "S", null, null);


        //creiamo una rete con solo il nostor sink
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend((StateBackend) new RocksDBStateBackend("/tmp/flinkTestStateRocksdb"));


        List<AugmentedWriteRequest> list = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            list.add(
                    createAugmentedWriteRequest("TempTableNameConflict", String.format("key-%04d", i), "XYZ"));
        }

        DataStreamSource<AugmentedWriteRequest> streamIn = env.fromCollection(list);

        Properties properties = new Properties();
        properties.setProperty(FlinkDynamoDBProducer.DYNAMODB_PRODUCER_CHECKPOINTINGMODE_PROPERTY,
                FlinkDynamoDBProducer.CheckpointingMode.ListState.name());

        streamIn.addSink(
                new FlinkDynamoDBProducer(properties, (v) -> "CONFLICTING_KEY") {
                    @Override
                    protected Client getClient(Properties producerProps) {
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
                .withTableName("TempTableNameConflict")
                .withAttributesToGet(Arrays.asList("KeyName", "anotherAttribute"));

        while (true) {
            ScanResult scanResult = dynamoDBClient.scan(sr);

            items.addAll(scanResult.getItems());
            if (scanResult.getLastEvaluatedKey() == null || scanResult.getLastEvaluatedKey().isEmpty())
                break;
            else
                sr.setExclusiveStartKey(scanResult.getLastEvaluatedKey());
        }
        assertEquals(30, items.size());
        System.err.println(items.size());
        System.err.println(items);
    }

    @Test
    public void testFail() throws Exception {

        String EXC_MESSAGE = "Intended Exception for testing";

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

            Properties properties = new Properties();
            properties.setProperty(FlinkDynamoDBProducer.DYNAMODB_PRODUCER_CHECKPOINTINGMODE_PROPERTY,
                    FlinkDynamoDBProducer.CheckpointingMode.ListState.name());

            FlinkDynamoDBProducer mySink = new FlinkDynamoDBProducer(properties) {
                @Override
                protected Client getClient(Properties producerProps) {
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


        } catch (Throwable e) {

            Throwable root = e;
            while (root.getCause() != null) {
                System.err.println("recurring from: " + root.getCause());
                root = root.getCause();
            }
            assertEquals(root.getMessage(), EXC_MESSAGE);

        }


    }


    public enum EntryType {
        ERROR,
        OFFLINE
    }

    @ToString
    @NoArgsConstructor
    @Getter
    @Accessors(chain = true)
    @DynamoDBTable(tableName = "_gvr-bic-devices-status-now")
    public static class NowDeviceEntry {

        @DynamoDBHashKey
        @Setter
        private String siteId;
        @Setter
        private String localKey;
        @Setter
        private String code;
        @Setter
        private String device;
        @DynamoDBTyped(DynamoDBMapperFieldModel.DynamoDBAttributeType.S)
        private DateTime in;
        @Setter
        private String parent;
        @Setter
        private boolean propagableToParent;
        @Setter
        private boolean propagableToSite;

        String getEntryOwner() {
            return isComponent() ? this.parent : this.device;
        }

        boolean isAnErrorEntry() {
            return !EntryType.OFFLINE.name().equals(this.code);
        }

        EntryType getType() {
            return isAnErrorEntry() ? EntryType.ERROR : EntryType.OFFLINE;
        }

        public void setIn(DateTime in) {
            this.in = in.withZone(DateTimeZone.UTC);
        }

        boolean isComponent() {
            return !StringUtils.isNullOrEmpty(this.parent);
        }

        public enum Properties {
            siteId,
            localKey,
            code,
            device,
            in,
            eventTime
        }

    }

    public static class AugmentedWriteRequestSelector implements KeySelector<AugmentedWriteRequest, String> {

        @Override
        public String getKey(AugmentedWriteRequest request) {

            final PutRequest putRequest = request.getRequest().getPutRequest();
            if (null != putRequest) {
                String putKey = putRequest.getItem()
                        .entrySet()
                        .stream()
                        .filter(entry -> Arrays.asList(NowDeviceEntry.Properties.siteId.name(), NowDeviceEntry.Properties.localKey.name()).contains(entry.getKey()))
                        .map(entry -> entry.getValue().getS())
                        .collect(Collectors.joining("_"));

                System.err.println("putKey: " + putKey);
                return putKey;
            }
            String delKey = request.getRequest()
                    .getDeleteRequest()
                    .getKey()
                    .values().stream()
                    .map(AttributeValue::getS)
                    .collect(Collectors.joining("_"));
            System.err.println("delKey: " + delKey);
            return delKey;

        }
    }


}
