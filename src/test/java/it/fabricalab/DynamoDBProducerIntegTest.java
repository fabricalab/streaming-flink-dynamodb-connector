package it.fabricalab;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.util.concurrent.ListenableFuture;
import it.fabricalab.flink.dynamodb.sink.AugmentedWriteRequest;
import it.fabricalab.flink.dynamodb.sink.DynamoDBProducer;
import it.fabricalab.flink.dynamodb.sink.WriteItemResult;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.testcontainers.dynamodb.DynaliteContainer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class DynamoDBProducerIntegTest {


    @ClassRule
    public static DynaliteContainer dynamoDB = new DynaliteContainer();

    private static final Consumer<Throwable> dummyCallback = (t) -> {
        System.err.println("CALLBACK: " + t);
        t.printStackTrace();
    };


    @Test
    void testWitSpiller() throws InterruptedException, ExecutionException, TimeoutException {
        dynamoDB.start();

        AmazonDynamoDB client = dynamoDB.getClient();
        TestUtils.createTable(client, "TempTableName", 5, 5,
                "KeyName", "S", null, null);

        DynamoDBProducer producer =
                new DynamoDBProducer(new Properties(), (r) -> client.batchWriteItem(r), dummyCallback);

        List<ListenableFuture> futures = new ArrayList<>();


        ListenableFuture<WriteItemResult> f;
        for (int i = 0; i < 25; i++) {

            HashMap<String, AttributeValue> item = new HashMap<>();
            item.put("KeyName", new AttributeValue("key_" + i));
            item.put("anotherAttribute", new AttributeValue(UUID.randomUUID().toString()));

            WriteRequest wr = new WriteRequest().withPutRequest(new PutRequest().withItem(item));

            f = producer.addUserRecord(new AugmentedWriteRequest("TempTableName", wr));
            futures.add(f);
            assertEquals(0, producer.getSpillerCycles());
        }

        f = producer.addUserRecord(new AugmentedWriteRequest("TempTableName", new WriteRequest()));
        futures.add(f);

        //now it will fire
        //wait for that
        futures.get(0).get();

        assertEquals(1, producer.getSpillerCycles());

        //spiller consumer a map of 25
        assertEquals(1, producer.getOutstandingRecordsCount());

        //one in the currentmap
        assertEquals(1,
                producer.getCurrentlyUnderConstruction().values().stream()
                        .collect(Collectors.summingInt(l -> l.size())));

        //no map in the queue
        assertEquals(0, producer.getQueue().size());


        //all the futures are done bit the last
        assertFalse(futures.get(futures.size() - 1).isDone());

        for (int i = 0; i < futures.size() - 1; i++)
            assertTrue(futures.get(i).isDone());


        //adesso guardiamo cosa c'e' nella tabella
        List<Map<String, AttributeValue>> items = new ArrayList<>();

        ScanRequest sr = new ScanRequest()
                .withTableName("TempTableName")
                .withAttributesToGet(Arrays.asList("KeyName", "anotherAttribute"));


        while (true) {
            ScanResult scanResult = client.scan( sr );

            items.addAll(scanResult.getItems());
            if(scanResult.getLastEvaluatedKey()  == null || scanResult.getLastEvaluatedKey().isEmpty() )
                break;
            else
                sr.setExclusiveStartKey( scanResult.getLastEvaluatedKey() );
        }

        assertEquals(items.size(), 25);
        System.err.println(items.size());
        System.err.println(items);

    }


}